pub mod grpc_plugin_source;
pub mod postgres_target;
pub mod websocket_source;

use {
    async_trait::async_trait,
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
    std::sync::Arc,
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
    pub postgres_connection_string: String,
    pub postgres_account_write_connections: i32,
    pub grpc_connection_string: String,
    pub rpc_http_url: String,
    pub rpc_ws_url: String,
    pub program_id: String,
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

pub struct RawAccountTable {}

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
