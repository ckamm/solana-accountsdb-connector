mod grpc_plugin_source;
mod postgres_target;
mod websocket_source;

use {
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
    log::*,
    std::{
        fs::File,
        io::Read,
    },
};

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

#[derive(Clone, PartialEq, Debug)]
pub struct SlotUpdate {
    pub slot: i64,
    pub parent: Option<i64>,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    postgres_connection_string: String,
    grpc_connection_string: String,
    rpc_http_url: String,
    rpc_ws_url: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("requires a config file argument");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        serde_json::from_str(&contents).unwrap()
    };

    solana_logger::setup_with_default("info");
    info!("startup");

    let (account_write_queue_sender, slot_queue_sender) =
        postgres_target::init(&config.postgres_connection_string);

    let use_accountsdb = true;
    if use_accountsdb {
        grpc_plugin_source::process_events(
            config,
            account_write_queue_sender,
            slot_queue_sender);
    } else {
        websocket_source::process_events(
            config,
            account_write_queue_sender,
            slot_queue_sender);
    }

    Ok(())
}
