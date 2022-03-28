use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::http;

use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_rpc::{rpc::rpc_accounts::AccountsDataClient, rpc::OptionalContext};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use futures::{future, future::FutureExt};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use log::*;
use std::{collections::HashMap, str::FromStr, time::Duration};

pub mod geyser_proto {
    tonic::include_proto!("accountsdb");
}
use geyser_proto::accounts_db_client::AccountsDbClient;

use crate::{
    metrics, AccountWrite, AnyhowWrap, GrpcSourceConfig, SlotStatus, SlotUpdate,
    SnapshotSourceConfig, SourceConfig, TlsConfig,
};

type SnapshotData = Response<Vec<RpcKeyedAccount>>;

enum Message {
    GrpcUpdate(geyser_proto::Update),
    Snapshot(SnapshotData),
}

async fn get_snapshot(
    rpc_http_url: String,
    program_id: Pubkey,
) -> anyhow::Result<OptionalContext<Vec<RpcKeyedAccount>>> {
    let rpc_client = http::connect_with_options::<AccountsDataClient>(&rpc_http_url, true)
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

async fn feed_data_geyser(
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
        .subscribe(geyser_proto::SubscribeRequest {})
        .await?
        .into_inner();

    // We can't get a snapshot immediately since the finalized snapshot would be for a
    // slot in the past and we'd be missing intermediate updates.
    //
    // Delay the request until the first slot we received all writes for becomes rooted
    // to avoid that problem - partially. The rooted slot will still be larger than the
    // finalized slot, so add a number of slots as a buffer.
    //
    // If that buffer isn't sufficient, there'll be a retry.

    // The first slot that we will receive _all_ account writes for
    let mut first_full_slot: u64 = u64::MAX;

    // If a snapshot should be performed when ready.
    let mut snapshot_needed = true;

    // The highest "rooted" slot that has been seen.
    let mut max_rooted_slot = 0;

    // Data for slots will arrive out of order. This value defines how many
    // slots after a slot was marked "rooted" we assume it'll not receive
    // any more account write information.
    //
    // This is important for the write_version mapping (to know when slots can
    // be dropped).
    let max_out_of_order_slots = 40;

    // Number of slots that we expect "finalized" commitment to lag
    // behind "rooted". This matters for getProgramAccounts based snapshots,
    // which will have "finalized" commitment.
    let mut rooted_to_finalized_slots = 30;

    let mut snapshot_future = future::Fuse::terminated();

    // The plugin sends a ping every 5s or so
    let fatal_idle_timeout = Duration::from_secs(60);

    // Highest slot that an account write came in for.
    let mut newest_write_slot: u64 = 0;

    struct WriteVersion {
        // Write version seen on-chain
        global: u64,
        // The per-pubkey per-slot write version
        slot: u32,
    }

    // map slot -> (pubkey -> WriteVersion)
    //
    // Since the write_version is a private indentifier per node it can't be used
    // to deduplicate events from multiple nodes. Here we rewrite it such that each
    // pubkey and each slot has a consecutive numbering of writes starting at 1.
    //
    // That number will be consistent for each node.
    let mut slot_pubkey_writes = HashMap::<u64, HashMap<[u8; 32], WriteVersion>>::new();

    loop {
        tokio::select! {
            update = update_stream.next() => {
                use geyser_proto::{update::UpdateOneof, slot_update::Status};
                let mut update = update.ok_or(anyhow::anyhow!("geyser plugin has closed the stream"))??;
                match update.update_oneof.as_mut().expect("invalid grpc") {
                    UpdateOneof::SubscribeResponse(subscribe_response) => {
                        first_full_slot = subscribe_response.highest_write_slot + 1;
                    },
                    UpdateOneof::SlotUpdate(slot_update) => {
                        let status = slot_update.status;
                        if status == Status::Rooted as i32 {
                            if slot_update.slot > max_rooted_slot {
                                max_rooted_slot = slot_update.slot;

                                // drop data for slots that are well beyond rooted
                                slot_pubkey_writes.retain(|&k, _| k >= max_rooted_slot - max_out_of_order_slots);
                            }
                            if snapshot_needed && max_rooted_slot - rooted_to_finalized_slots > first_full_slot {
                                snapshot_needed = false;
                                snapshot_future = tokio::spawn(get_snapshot(snapshot_config.rpc_http_url.clone(), program_id)).fuse();
                            }
                        }
                    },
                    UpdateOneof::AccountWrite(write) => {
                        if write.slot < first_full_slot {
                            // Don't try to process data for slots where we may have missed writes:
                            // We could not map the write_version correctly for them.
                            continue;
                        }

                        if write.slot > newest_write_slot {
                            newest_write_slot = write.slot;
                        } else if max_rooted_slot > 0 && write.slot < max_rooted_slot - max_out_of_order_slots {
                            anyhow::bail!("received write {} slots back from max rooted slot {}", max_rooted_slot - write.slot, max_rooted_slot);
                        }

                        let pubkey_writes = slot_pubkey_writes.entry(write.slot).or_default();

                        let pubkey_bytes = Pubkey::new(&write.pubkey).to_bytes();
                        let write_version_mapping = pubkey_writes.entry(pubkey_bytes).or_insert(WriteVersion {
                            global: write.write_version,
                            slot: 1, // write version 0 is reserved for snapshots
                        });

                        // We assume we will receive write versions for each pubkey in sequence.
                        // If this is not the case, logic here does not work correctly because
                        // a later write could arrive first.
                        if write.write_version < write_version_mapping.global {
                            anyhow::bail!("unexpected write version: got {}, expected >= {}", write.write_version, write_version_mapping.global);
                        }

                        // Rewrite the update to use the local write version and bump it
                        write.write_version = write_version_mapping.slot as u64;
                        write_version_mapping.slot += 1;
                    },
                    geyser_proto::update::UpdateOneof::Ping(_) => {},
                }
                sender.send(Message::GrpcUpdate(update)).await.expect("send success");
            },
            snapshot = &mut snapshot_future => {
                let snapshot = snapshot??;
                if let OptionalContext::Context(snapshot_data) = snapshot {
                    info!("snapshot is for slot {}, first full slot was {}", snapshot_data.context.slot, first_full_slot);
                    if snapshot_data.context.slot >= first_full_slot {
                        sender
                        .send(Message::Snapshot(snapshot_data))
                        .await
                        .expect("send success");
                    } else {
                        info!(
                            "snapshot is too old: has slot {}, expected {} minimum",
                            snapshot_data.context.slot,
                            first_full_slot
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
                anyhow::bail!("geyser plugin hasn't sent a message in too long");
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
    config: &SourceConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
    metrics_sender: metrics::Metrics,
) {
    // Subscribe to geyser
    let (msg_sender, msg_receiver) = async_channel::bounded::<Message>(config.dedup_queue_size);
    for grpc_source in config.grpc_sources.clone() {
        let msg_sender = msg_sender.clone();
        let snapshot_source = config.snapshot.clone();
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
                let out = feed_data_geyser(
                    &grpc_source,
                    tls_config.clone(),
                    &snapshot_source,
                    msg_sender.clone(),
                );
                let result = out.await;
                assert!(result.is_err());
                if let Err(err) = result {
                    warn!(
                        "error during communication with the geyser plugin. retrying. {:?}",
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

    // slot -> (pubkey -> write_version)
    //
    // To avoid unnecessarily sending requests to SQL, we track the latest write_version
    // for each (slot, pubkey). If an already-seen write_version comes in, it can be safely
    // discarded.
    let mut latest_write = HashMap::<u64, HashMap<[u8; 32], u64>>::new();

    // Number of slots to retain in latest_write
    let latest_write_retention = 50;

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
                    geyser_proto::update::UpdateOneof::AccountWrite(update) => {
                        assert!(update.pubkey.len() == 32);
                        assert!(update.owner.len() == 32);

                        metric_account_writes.increment();
                        metric_account_queue.set(account_write_queue_sender.len() as u64);

                        // Skip writes that a different server has already sent
                        let pubkey_writes = latest_write.entry(update.slot).or_default();
                        let pubkey_bytes = Pubkey::new(&update.pubkey).to_bytes();
                        let writes = pubkey_writes.entry(pubkey_bytes).or_insert(0);
                        if update.write_version <= *writes {
                            continue;
                        }
                        *writes = update.write_version;
                        latest_write.retain(|&k, _| k >= update.slot - latest_write_retention);

                        account_write_queue_sender
                            .send(AccountWrite {
                                pubkey: Pubkey::new(&update.pubkey),
                                slot: update.slot,
                                write_version: update.write_version,
                                lamports: update.lamports,
                                owner: Pubkey::new(&update.owner),
                                executable: update.executable,
                                rent_epoch: update.rent_epoch,
                                data: update.data,
                                is_selected: update.is_selected,
                            })
                            .await
                            .expect("send success");
                    }
                    geyser_proto::update::UpdateOneof::SlotUpdate(update) => {
                        metric_slot_updates.increment();
                        metric_slot_queue.set(slot_queue_sender.len() as u64);

                        use geyser_proto::slot_update::Status;
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
                            slot: update.slot,
                            parent: update.parent,
                            status: status.expect("qed"),
                        };

                        slot_queue_sender
                            .send(slot_update)
                            .await
                            .expect("send success");
                    }
                    geyser_proto::update::UpdateOneof::Ping(_) => {}
                    geyser_proto::update::UpdateOneof::SubscribeResponse(_) => {}
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
