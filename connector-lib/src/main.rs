mod grpc_plugin_source;
mod postgres_target;
mod websocket_source;

mod mango;

use {
    async_trait::async_trait,
    log::*,
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
    std::{fs::File, io::Read, sync::Arc},
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

impl AccountWrite {
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

#[async_trait]
pub trait AccountTable: Sync + Send {
    fn table_name(&self) -> &str;
    async fn insert_account_write(
        &self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        account_write: &AccountWrite,
    ) -> Result<(), anyhow::Error>;
}

pub type AccountTables = Vec<Arc<dyn AccountTable>>;

struct RawAccountTable {}

pub fn encode_address(addr: &Pubkey) -> String {
    bs58::encode(&addr.to_bytes()).into_string()
}

#[async_trait]
impl AccountTable for RawAccountTable {
    fn table_name(&self) -> &str {
        "account_write"
    }

    async fn insert_account_write(
        &self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        account_write: &AccountWrite,
    ) -> Result<(), anyhow::Error> {
        let pubkey = encode_address(&account_write.pubkey);
        let owner = encode_address(&account_write.owner);

        // TODO: should update for same write_version to work with websocket input
        let query = postgres_query::query!(
            "
            INSERT INTO account_write
            (pubkey, slot, write_version, owner, lamports, executable, rent_epoch, data)
            VALUES
            ($pubkey, $slot, $write_version, $owner, $lamports, $executable, $rent_epoch, $data)
            ON CONFLICT (pubkey, slot, write_version) DO NOTHING",
            pubkey,
            slot = account_write.slot,
            write_version = account_write.write_version,
            owner,
            lamports = account_write.lamports,
            executable = account_write.executable,
            rent_epoch = account_write.rent_epoch,
            data = account_write.data,
        );
        let _ = query.execute(client).await?;
        Ok(())
    }
}

use postgres_types::ToSql;
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

    let account_tables: AccountTables = vec![
        Arc::new(RawAccountTable {}),
        Arc::new(mango::MangoAccountTable {}),
        Arc::new(mango::MangoGroupTable {}),
        Arc::new(mango::MangoCacheTable {}),
    ];
    //let account_tables: AccountTables = vec![Arc::new(RawAccountTable {})];

    let (account_write_queue_sender, slot_queue_sender) =
        postgres_target::init(&config.postgres_connection_string, account_tables).await?;

    info!("postgres done");
    let use_accountsdb = true;
    if use_accountsdb {
        grpc_plugin_source::process_events(config, account_write_queue_sender, slot_queue_sender)
            .await;
    } else {
        websocket_source::process_events(config, account_write_queue_sender, slot_queue_sender)
            .await;
    }

    Ok(())
}
