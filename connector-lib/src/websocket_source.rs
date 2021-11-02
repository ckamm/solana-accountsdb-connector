use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::{http, ws};

use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    //rpc_filter::RpcFilterType,
    rpc_response::{Response, RpcKeyedAccount},
};
use solana_rpc::{rpc::rpc_full::FullClient, rpc::OptionalContext, rpc_pubsub::RpcSolPubSubClient};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use log::*;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{AccountWrite, AnyhowWrap, SlotUpdate};

impl crate::AccountWrite {
    fn from(pubkey: Pubkey, slot: u64, write_version: i64, account: Account) -> AccountWrite {
        AccountWrite {
            pubkey,
            slot: slot as i64, // TODO: narrowing!
            write_version,
            lamports: account.lamports as i64, // TODO: narrowing!
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch as i64, // TODO: narrowing!
            data: account.data.clone(),
        }
    }
}

enum WebsocketMessage {
    SingleUpdate(Response<RpcKeyedAccount>),
    SnapshotUpdate(Response<Vec<RpcKeyedAccount>>),
    SlotUpdate(Arc<solana_client::rpc_response::SlotUpdate>),
}

// TODO: the reconnecting should be part of this
async fn feed_data(
    sender: crossbeam_channel::Sender<WebsocketMessage>,
) -> Result<(), anyhow::Error> {
    let rpc_pubsub_url = "";
    let rpc_http_url = "";
    let program_id = Pubkey::from_str("mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68")?;
    let mango_group_address = Pubkey::from_str("98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue")?;
    let snapshot_duration = Duration::from_secs(300);

    let connect = ws::try_connect::<RpcSolPubSubClient>(&rpc_pubsub_url).map_err_anyhow()?;
    let client = connect.await.map_err_anyhow()?;

    let rpc_client = http::connect_with_options::<FullClient>(&rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
    };
    // TODO: Make addresses filters configurable
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None, /*Some(vec![RpcFilterType::DataSize(
                           size_of::<MangoAccount>() as u64
                       )]),*/
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    let mut update_sub = client
        .program_subscribe(
            program_id.to_string(),
            Some(program_accounts_config.clone()),
        )
        .map_err_anyhow()?;
    let mut slot_sub = client.slots_updates_subscribe().map_err_anyhow()?;

    let mut last_snapshot = Instant::now() - snapshot_duration;

    loop {
        // occasionally cause a new snapshot to be produced
        // including the first time
        if last_snapshot + snapshot_duration <= Instant::now() {
            let account_snapshot = rpc_client
                .get_program_accounts(
                    program_id.to_string(),
                    Some(program_accounts_config.clone()),
                )
                .await
                .map_err_anyhow()?;
            if let OptionalContext::Context(account_snapshot_response) = account_snapshot {
                sender
                    .send(WebsocketMessage::SnapshotUpdate(account_snapshot_response))
                    .expect("sending must succeed");
            }
            last_snapshot = Instant::now();
        }

        tokio::select! {
            account = update_sub.next() => {
                match account {
                    Some(account) => {
                        sender.send(WebsocketMessage::SingleUpdate(account.map_err_anyhow()?)).expect("sending must succeed");
                    },
                    None => {
                        warn!("account stream closed");
                        return Ok(());
                    },
                }
            },
            slot_update = slot_sub.next() => {
                match slot_update {
                    Some(slot_update) => {
                        sender.send(WebsocketMessage::SlotUpdate(slot_update.map_err_anyhow()?)).expect("sending must succeed");
                    },
                    None => {
                        warn!("slot update stream closed");
                        return Ok(());
                    },
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                warn!("websocket timeout");
                return Ok(())
            }
        }
    }
}

// TODO: rename / split / rework
pub fn process_events(
    account_write_queue_sender: crossbeam_channel::Sender<AccountWrite>,
    slot_queue_sender: crossbeam_channel::Sender<SlotUpdate>,
) {
    // Subscribe to program account updates websocket
    let (update_sender, update_receiver) = crossbeam_channel::unbounded::<WebsocketMessage>();
    tokio::spawn(async move {
        // if the websocket disconnects, we get no data in a while etc, reconnect and try again
        loop {
            let out = feed_data(update_sender.clone());
            let _ = out.await;
        }
    });

    //
    // The thread that pulls updates and forwards them to postgres
    //

    // copy websocket updates into the postgres account write queue
    loop {
        let update = update_receiver.recv().unwrap();
        info!("got update message");

        match update {
            WebsocketMessage::SingleUpdate(update) => {
                info!("single update");
                let account: Account = update.value.account.decode().unwrap();
                let pubkey = Pubkey::from_str(&update.value.pubkey).unwrap();
                account_write_queue_sender
                    .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                    .unwrap();
            }
            WebsocketMessage::SnapshotUpdate(update) => {
                info!("snapshot update");
                for keyed_account in update.value {
                    let account: Account = keyed_account.account.decode().unwrap();
                    let pubkey = Pubkey::from_str(&keyed_account.pubkey).unwrap();
                    account_write_queue_sender
                        .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
                        .unwrap();
                }
            }
            WebsocketMessage::SlotUpdate(update) => {
                info!("slot update");
                let message = match *update {
                    solana_client::rpc_response::SlotUpdate::CreatedBank {
                        slot, parent, ..
                    } => Some(SlotUpdate {
                        slot: slot as i64, // TODO: narrowing
                        parent: Some(parent as i64),
                        status: "processed".into(),
                    }),
                    solana_client::rpc_response::SlotUpdate::OptimisticConfirmation {
                        slot,
                        ..
                    } => Some(SlotUpdate {
                        slot: slot as i64, // TODO: narrowing
                        parent: None,
                        status: "confirmed".into(),
                    }),
                    solana_client::rpc_response::SlotUpdate::Root { slot, .. } => {
                        Some(SlotUpdate {
                            slot: slot as i64, // TODO: narrowing
                            parent: None,
                            status: "rooted".into(),
                        })
                    }
                    _ => None,
                };
                if let Some(message) = message {
                    slot_queue_sender.send(message).unwrap();
                }
            }
        }
    }
}
