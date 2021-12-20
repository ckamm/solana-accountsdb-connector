use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::http;

use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_rpc::{rpc::rpc_full::FullClient, rpc::OptionalContext};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use futures::{future, future::FutureExt};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use log::*;
use std::{collections::HashMap, str::FromStr, time::Duration};

pub mod accountsdb_proto {
    tonic::include_proto!("accountsdb");
}
use accountsdb_proto::accounts_db_client::AccountsDbClient;

use crate::{
    metrics, AccountWrite, AnyhowWrap, Config, GrpcSourceConfig, SlotStatus, SlotUpdate,
    SnapshotSourceConfig, TlsConfig,
};

type SnapshotData = Response<Vec<RpcKeyedAccount>>;

enum Message {
    GrpcUpdate(accountsdb_proto::Update),
    Snapshot(SnapshotData),
}

async fn get_snapshot(
    rpc_http_url: String,
    program_id: Pubkey,
) -> anyhow::Result<OptionalContext<Vec<RpcKeyedAccount>>> {
    let rpc_client = http::connect_with_options::<FullClient>(&rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
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
    info!("snapshot received");
    Ok(account_snapshot)
}

async fn feed_data_accountsdb(
    grpc_config: &GrpcSourceConfig,
    tls_config: Option<ClientTlsConfig>,
    snapshot_config: &SnapshotSourceConfig,
    sender: async_channel::Sender<Message>,
) -> anyhow::Result<()> {
    let program_id = Pubkey::from_str(&snapshot_config.program_id)?;

    let endpoint = Endpoint::from_str(&grpc_config.connection_string)?;
    let channel = if let Some(tls) = tls_config {
        endpoint.tls_config(tls)?
    } else {
        endpoint
    }
    .connect()
    .await?;
    let mut client = AccountsDbClient::new(channel);

    let mut update_stream = client
        .subscribe(accountsdb_proto::SubscribeRequest {})
        .await?
        .into_inner();

    // We can't get a snapshot immediately since the finalized snapshot would be for a
    // slot in the past and we'd be missing intermediate updates.
    //
    // Delay the request until the first processed slot we heard about becomes rooted
    // to avoid that problem - partially. The rooted slot will still be larger than the
    // finalized slot, so add a number of slots as a buffer.
    //
    // If that buffer isn't sufficient, there'll be a retry.

    // If a snapshot should be performed when ready.
    let mut snapshot_needed = true;

    // Lowest slot that an account write was received for.
    // The slot one after that will have received all write events.
    let mut lowest_write_slot = u64::MAX;

    // Number of slots that we expect "finalized" commitment to lag
    // behind "rooted".
    let mut rooted_to_finalized_slots = 30;

    let mut snapshot_future = future::Fuse::terminated();

    // The plugin sends a ping every 5s or so
    let fatal_idle_timeout = Duration::from_secs(60);

    // Current slot that account writes come in for.
    let mut current_write_slot: u64 = 0;
    // Unfortunately the write_version the plugin provides is local to
    // each RPC node. Here we fix it up by giving each pubkey a write_version
    // based on the count of writes it gets each slot.
    let mut slot_pubkey_writes = HashMap::<Vec<u8>, u64>::new();

    // Keep track of write version from RPC node, to check assumptions
    let mut last_write_version: u64 = 0;

    loop {
        tokio::select! {
            update = update_stream.next() => {
                use accountsdb_proto::{update::UpdateOneof, slot_update::Status};
                let mut update = update.ok_or(anyhow::anyhow!("accountsdb plugin has closed the stream"))??;
                match update.update_oneof.as_mut().expect("invalid grpc") {
                    UpdateOneof::SlotUpdate(slot_update) => {
                        let status = slot_update.status;
                        if snapshot_needed && status == Status::Rooted as i32 && slot_update.slot - rooted_to_finalized_slots > lowest_write_slot {
                            snapshot_needed = false;
                            snapshot_future = tokio::spawn(get_snapshot(snapshot_config.rpc_http_url.clone(), program_id)).fuse();
                        }
                    },
                    UpdateOneof::AccountWrite(write) => {
                        if write.slot > current_write_slot {
                            current_write_slot = write.slot;
                            slot_pubkey_writes.clear();
                            if lowest_write_slot > write.slot {
                                lowest_write_slot = write.slot;
                            }
                        }
                        if lowest_write_slot == write.slot {
                            // don't send out account writes for the first slot
                            // since we don't know their write_version
                            continue;
                        }

                        // We assume we will receive write versions in sequence.
                        // If this is not the case, logic here does not work correctly because
                        // a later write could arrive first.
                        assert!(write.write_version > last_write_version);
                        last_write_version = write.write_version;

                        let version = slot_pubkey_writes.entry(write.pubkey.clone()).or_insert(0);
                        write.write_version = *version;
                        *version += 1;
                    },
                    accountsdb_proto::update::UpdateOneof::Ping(_) => {},
                }
                sender.send(Message::GrpcUpdate(update)).await.expect("send success");
            },
            snapshot = &mut snapshot_future => {
                let snapshot = snapshot??;
                if let OptionalContext::Context(snapshot_data) = snapshot {
                    info!("snapshot is for slot {}, min write slot was {}", snapshot_data.context.slot, lowest_write_slot);
                    if snapshot_data.context.slot >= lowest_write_slot + 1 {
                        sender
                        .send(Message::Snapshot(snapshot_data))
                        .await
                        .expect("send success");
                    } else {
                        info!(
                            "snapshot is too old: has slot {}, expected {} minimum",
                            snapshot_data.context.slot,
                            lowest_write_slot + 1
                        );
                        // try again in another 10 slots
                        snapshot_needed = true;
                        rooted_to_finalized_slots += 10;
                    }
                } else {
                    anyhow::bail!("bad snapshot format");
                }
            },
            _ = tokio::time::sleep(fatal_idle_timeout) => {
                anyhow::bail!("accountsdb plugin hasn't sent a message in too long");
            }
        }
    }
}

fn make_tls_config(config: &TlsConfig) -> ClientTlsConfig {
    let server_root_ca_cert =
        std::fs::read(&config.ca_cert_path).expect("reading server root ca cert");
    let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
    let client_cert = std::fs::read(&config.client_cert_path).expect("reading client cert");
    let client_key = std::fs::read(&config.client_key_path).expect("reading client key");
    let client_identity = Identity::from_pem(client_cert, client_key);
    ClientTlsConfig::new()
        .ca_certificate(server_root_ca_cert)
        .identity(client_identity)
        .domain_name(&config.domain_name)
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

        // Make TLS config if configured
        let tls_config = grpc_source.tls.as_ref().map(make_tls_config);

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
                let out = feed_data_accountsdb(
                    &grpc_source,
                    tls_config.clone(),
                    &snapshot_source,
                    msg_sender.clone(),
                );
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
                        let slot_update = SlotUpdate {
                            slot: update.slot as i64, // TODO: narrowing
                            parent: update.parent.map(|v| v as i64),
                            status: status.expect("qed"),
                        };

                        slot_queue_sender
                            .send(slot_update)
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
