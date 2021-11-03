use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::http;

use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_rpc::{rpc::rpc_full::FullClient, rpc::OptionalContext};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use tonic::transport::Endpoint;

use log::*;
use std::{str::FromStr, time::Duration};

pub mod accountsdb_proto {
    tonic::include_proto!("accountsdb");
}
use accountsdb_proto::accounts_db_client::AccountsDbClient;

use crate::{AccountWrite, AnyhowWrap, Config, SlotUpdate};

enum Message {
    GrpcUpdate(accountsdb_proto::Update),
    Snapshot(Response<Vec<RpcKeyedAccount>>),
}

async fn feed_data_accountsdb(
    config: &Config,
    sender: async_channel::Sender<Message>,
) -> Result<(), anyhow::Error> {
    let mut client =
        AccountsDbClient::connect(Endpoint::from_str(&config.grpc_connection_string)?).await?;

    let mut update_stream = client
        .subscribe(accountsdb_proto::SubscribeRequest {})
        .await?
        .into_inner();

    let rpc_client = http::connect_with_options::<FullClient>(&config.rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let program_id = Pubkey::from_str("mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68")?;
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

    // Get an account snapshot on start
    // TODO: Should only do that once we know we have all account write events for that slot
    info!("requesting snapshot");
    let account_snapshot = rpc_client
        .get_program_accounts(
            program_id.to_string(),
            Some(program_accounts_config.clone()),
        )
        .await
        .map_err_anyhow()?;
    if let OptionalContext::Context(account_snapshot_response) = account_snapshot {
        sender
            .send(Message::Snapshot(account_snapshot_response))
            .await
            .expect("send success");
    } else {
        anyhow::bail!("bad snapshot format");
    }
    info!("snapshot done");

    loop {
        tokio::select! {
            update = update_stream.next() => {
                match update {
                    Some(update) => {
                        sender.send(Message::GrpcUpdate(update?)).await.expect("send success");
                    },
                    None => {
                        anyhow::bail!("accountsdb plugin has closed the stream");
                    },
                }
            },
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                anyhow::bail!("accountsdb plugin hasn't sent a message in too long");
            }
        }
    }
}

pub async fn process_events(
    config: Config,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
) {
    // Subscribe to accountsdb
    let (msg_sender, msg_receiver) = async_channel::unbounded::<Message>();
    tokio::spawn(async move {
        // Continuously reconnect on failure
        loop {
            let out = feed_data_accountsdb(&config, msg_sender.clone());
            let result = out.await;
            assert!(result.is_err());
            if let Err(err) = result {
                warn!(
                    "error during communication with the accountsdb plugin. retrying. {:?}",
                    err
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    loop {
        let msg = msg_receiver.recv().await.unwrap();

        match msg {
            Message::GrpcUpdate(update) => {
                match update.update_oneof.unwrap() {
                    accountsdb_proto::update::UpdateOneof::AccountWrite(update) => {
                        assert!(update.pubkey.len() == 32);
                        assert!(update.owner.len() == 32);
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
                        use accountsdb_proto::slot_update::Status;
                        let status_string = match Status::from_i32(update.status) {
                            Some(Status::Processed) => "processed",
                            Some(Status::Confirmed) => "confirmed",
                            Some(Status::Rooted) => "rooted",
                            None => "",
                        };
                        if status_string == "" {
                            error!("unexpected slot status: {}", update.status);
                            continue;
                        }
                        slot_queue_sender
                            .send(SlotUpdate {
                                slot: update.slot as i64, // TODO: narrowing
                                parent: update.parent.map(|v| v as i64),
                                status: status_string.into(),
                            })
                            .await
                            .expect("send success");
                    }
                    accountsdb_proto::update::UpdateOneof::Ping(_) => {}
                }
            }
            Message::Snapshot(update) => {
                info!("processing snapshot...");
                for keyed_account in update.value {
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
