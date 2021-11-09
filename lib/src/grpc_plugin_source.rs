use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::http;

use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_rpc::{rpc::rpc_full::FullClient, rpc::OptionalContext};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use futures::{future, future::FutureExt};
use tonic::transport::Endpoint;

use log::*;
use std::{collections::HashMap, str::FromStr, time::Duration};

pub mod accountsdb_proto {
    tonic::include_proto!("accountsdb");
}
use accountsdb_proto::accounts_db_client::AccountsDbClient;

use crate::{
    metrics, AccountWrite, AnyhowWrap, Config, GrpcSourceConfig, SlotStatus, SlotUpdate,
    SnapshotSourceConfig,
};

type SnapshotData = Response<Vec<RpcKeyedAccount>>;

enum Message {
    GrpcUpdate(accountsdb_proto::Update),
    Snapshot(SnapshotData),
}

async fn get_snapshot(
    rpc_http_url: String,
    program_id: Pubkey,
    min_slot: u64,
) -> Result<SnapshotData, anyhow::Error> {
    let rpc_client = http::connect_with_options::<FullClient>(&rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    info!("requesting snapshot");
    let account_snapshot = rpc_client
        .get_program_accounts(
            program_id.to_string(),
            Some(program_accounts_config.clone()),
        )
        .await
        .map_err_anyhow()?;
    info!("snapshot done");
    if let OptionalContext::Context(account_snapshot_response) = account_snapshot {
        if account_snapshot_response.context.slot < min_slot {
            anyhow::bail!(
                "snapshot has slot {}, expected {} minimum",
                account_snapshot_response.context.slot,
                min_slot
            );
        }
        return Ok(account_snapshot_response);
    }

    anyhow::bail!("bad snapshot format");
}

async fn feed_data_accountsdb(
    grpc_config: &GrpcSourceConfig,
    snapshot_config: &SnapshotSourceConfig,
    sender: async_channel::Sender<Message>,
) -> Result<(), anyhow::Error> {
    let program_id = Pubkey::from_str(&snapshot_config.program_id)?;

    let mut client =
        AccountsDbClient::connect(Endpoint::from_str(&grpc_config.connection_string)?).await?;

    let mut update_stream = client
        .subscribe(accountsdb_proto::SubscribeRequest {})
        .await?
        .into_inner();

    // We can't get a snapshot immediately since the snapshot data has no write_version.
    // If we did, there could be missing account writes between the snapshot and
    // the first streamed data.
    // So instead, get a snapshot once we got notified about a new slot. Then we can
    // be confident that the snapshot will be for a slot >= that slot and that we'll have
    // all data for it.
    // We can't do it immediately for the first processed slot we get, because the
    // info about the new slot is sent before it's completed and the snapshot will be
    // for the preceding slot then. Thus wait for two slots, before asking for a snapshot.
    let trigger_snapshot_after_slots = 2;
    let mut trigger_snapshot_slot_counter = trigger_snapshot_after_slots;
    let mut snapshot_future = future::Fuse::terminated();

    // The plugin sends a ping every 5s or so
    let fatal_idle_timeout = Duration::from_secs(60);

    loop {
        tokio::select! {
            update = update_stream.next() => {
                match update {
                    Some(update) => {
                        use accountsdb_proto::{update::UpdateOneof, slot_update::Status};
                        let update = update?;
                        if let UpdateOneof::SlotUpdate(slot_update) = update.update_oneof.as_ref().expect("invalid grpc") {
                            if slot_update.status == Status::Processed as i32 {
                                if trigger_snapshot_slot_counter > 1 {
                                    trigger_snapshot_slot_counter -= 1;
                                } else if trigger_snapshot_slot_counter == 1 {
                                    snapshot_future = tokio::spawn(get_snapshot(snapshot_config.rpc_http_url.clone(), program_id, slot_update.slot - trigger_snapshot_after_slots + 1)).fuse();
                                    trigger_snapshot_slot_counter = 0;
                                }
                            }
                        }
                        sender.send(Message::GrpcUpdate(update)).await.expect("send success");
                    },
                    None => {
                        anyhow::bail!("accountsdb plugin has closed the stream");
                    },
                }
            },
            snapshot = &mut snapshot_future => {
                sender
                .send(Message::Snapshot(snapshot??))
                .await
                .expect("send success");
            },
            _ = tokio::time::sleep(fatal_idle_timeout) => {
                anyhow::bail!("accountsdb plugin hasn't sent a message in too long");
            }
        }
    }
}

pub async fn process_events(
    config: Config,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
    metrics_sender: metrics::Metrics,
) {
    // Subscribe to accountsdb
    let (msg_sender, msg_receiver) = async_channel::unbounded::<Message>();
    for grpc_source in config.grpc_sources {
        let msg_sender = msg_sender.clone();
        let snapshot_source = config.snapshot_source.clone();
        let metrics_sender = metrics_sender.clone();
        tokio::spawn(async move {
            let mut metric_retries = metrics_sender.register_u64(format!(
                "grpc_source_{}_connection_retries",
                grpc_source.name
            ));
            let metric_status =
                metrics_sender.register_string(format!("grpc_source_{}_status", grpc_source.name));

            // Continuously reconnect on failure
            loop {
                metric_status.set("connected".into());
                let out = feed_data_accountsdb(&grpc_source, &snapshot_source, msg_sender.clone());
                let result = out.await;
                assert!(result.is_err());
                if let Err(err) = result {
                    warn!(
                        "error during communication with the accountsdb plugin. retrying. {:?}",
                        err
                    );
                }

                metric_status.set("disconnected".into());
                metric_retries.increment();

                tokio::time::sleep(std::time::Duration::from_secs(
                    grpc_source.retry_connection_sleep_secs,
                ))
                .await;
            }
        });
    }

    let mut latest_write = HashMap::<Vec<u8>, (u64, u64)>::new();
    let mut metric_account_writes = metrics_sender.register_u64("grpc_account_writes".into());
    let mut metric_account_queue = metrics_sender.register_u64("account_write_queue".into());
    let mut metric_slot_queue = metrics_sender.register_u64("slot_update_queue".into());
    let mut metric_slot_updates = metrics_sender.register_u64("grpc_slot_updates".into());
    let mut metric_snapshots = metrics_sender.register_u64("grpc_snapshots".into());
    let mut metric_snapshot_account_writes =
        metrics_sender.register_u64("grpc_snapshot_account_writes".into());

    loop {
        let msg = msg_receiver.recv().await.expect("sender must not close");

        match msg {
            Message::GrpcUpdate(update) => {
                match update.update_oneof.expect("invalid grpc") {
                    accountsdb_proto::update::UpdateOneof::AccountWrite(update) => {
                        assert!(update.pubkey.len() == 32);
                        assert!(update.owner.len() == 32);

                        metric_account_writes.increment();
                        metric_account_queue.set(account_write_queue_sender.len() as u64);

                        // Each validator produces writes in strictly monotonous order.
                        // This early-out allows skipping postgres queries for the node
                        // that is behind.
                        if let Some((slot, write_version)) = latest_write.get(&update.pubkey) {
                            if *slot > update.slot
                                || (*slot == update.slot && *write_version > update.write_version)
                            {
                                continue;
                            }
                        }
                        latest_write
                            .insert(update.pubkey.clone(), (update.slot, update.write_version));

                        account_write_queue_sender
                            .send(AccountWrite {
                                pubkey: Pubkey::new(&update.pubkey),
                                slot: update.slot as i64, // TODO: narrowing
                                write_version: update.write_version as i64,
                                lamports: update.lamports as i64,
                                owner: Pubkey::new(&update.owner),
                                executable: update.executable,
                                rent_epoch: update.rent_epoch as i64,
                                data: update.data,
                            })
                            .await
                            .expect("send success");
                    }
                    accountsdb_proto::update::UpdateOneof::SlotUpdate(update) => {
                        metric_slot_updates.increment();
                        metric_slot_queue.set(slot_queue_sender.len() as u64);

                        use accountsdb_proto::slot_update::Status;
                        let status = Status::from_i32(update.status).map(|v| match v {
                            Status::Processed => SlotStatus::Processed,
                            Status::Confirmed => SlotStatus::Confirmed,
                            Status::Rooted => SlotStatus::Rooted,
                        });
                        if status.is_none() {
                            error!("unexpected slot status: {}", update.status);
                            continue;
                        }
                        slot_queue_sender
                            .send(SlotUpdate {
                                slot: update.slot as i64, // TODO: narrowing
                                parent: update.parent.map(|v| v as i64),
                                status: status.expect("qed"),
                            })
                            .await
                            .expect("send success");
                    }
                    accountsdb_proto::update::UpdateOneof::Ping(_) => {}
                }
            }
            Message::Snapshot(update) => {
                metric_snapshots.increment();
                info!("processing snapshot...");
                for keyed_account in update.value {
                    metric_snapshot_account_writes.increment();
                    metric_account_queue.set(account_write_queue_sender.len() as u64);

                    // TODO: Resnapshot on invalid data?
                    let account: Account = keyed_account.account.decode().unwrap();
                    let pubkey = Pubkey::from_str(&keyed_account.pubkey).unwrap();
                    account_write_queue_sender
                        .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                        .await
                        .expect("send success");
                }
                info!("processing snapshot done");
            }
        }
    }
}
