mod grpc_plugin_source;
mod postgres_target;
mod websocket_source;

use solana_sdk::pubkey::Pubkey;

use log::*;

trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct AccountWrite {
    pub pubkey: Pubkey,
    pub slot: i64,
    pub write_version: i64,
    pub lamports: i64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct SlotUpdate {
    pub slot: i64,
    pub parent: Option<i64>,
    pub status: String,
}

#[tokio::main]
async fn main() {
    solana_logger::setup_with_default("info");
    info!("startup");

    let postgres_connection_string = "host=/var/run/postgresql user=kamm port=5433";
    let (account_write_queue_sender, slot_queue_sender) =
        postgres_target::init(postgres_connection_string.into());

    let use_accountsdb = true;
    if use_accountsdb {
        grpc_plugin_source::process_events(account_write_queue_sender, slot_queue_sender);
    } else {
        websocket_source::process_events(account_write_queue_sender, slot_queue_sender);
    }
}
