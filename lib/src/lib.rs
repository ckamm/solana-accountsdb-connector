pub mod grpc_plugin_source;
pub mod metrics;
pub mod postgres_target;
pub mod postgres_types_numeric;
pub mod websocket_source;

use {
    async_trait::async_trait,
    postgres_types::ToSql,
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
            data: account.data,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, ToSql)]
pub enum SlotStatus {
    Rooted,
    Confirmed,
    Processed,
}

#[derive(Clone, Debug)]
pub struct SlotUpdate {
    pub slot: i64,
    pub parent: Option<i64>,
    pub status: SlotStatus,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
    /// Number of parallel postgres connections used for account write insertions
    pub account_write_connection_count: u64,
    /// Maximum batch size for account write inserts over one connection
    pub account_write_max_batch_size: usize,
    /// Number of parallel postgres connections used for slot insertions
    pub slot_update_connection_count: u64,
    /// Number of queries retries before fatal error
    pub retry_query_max_count: u64,
    /// Seconds to sleep between query retries
    pub retry_query_sleep_secs: u64,
    /// Seconds to sleep between connection attempts
    pub retry_connection_sleep_secs: u64,
    /// Fatal error when the connection can't be reestablished this long
    pub fatal_connection_timeout_secs: u64,
    /// Allow invalid TLS certificates, passed to native_tls danger_accept_invalid_certs
    pub allow_invalid_certs: bool,
    /// Delete old data automatically, keeping only the current data snapshot
    pub delete_old_data: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TlsConfig {
    pub ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub domain_name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GrpcSourceConfig {
    pub name: String,
    pub connection_string: String,
    pub retry_connection_sleep_secs: u64,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotSourceConfig {
    pub rpc_http_url: String,
    pub program_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub postgres_target: PostgresConfig,
    pub grpc_sources: Vec<GrpcSourceConfig>,
    pub snapshot_source: SnapshotSourceConfig,
    pub rpc_ws_url: String,
}

#[async_trait]
pub trait AccountTable: Sync + Send {
    fn table_name(&self) -> &str;
    async fn insert_account_write(
        &self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        account_write: &AccountWrite,
    ) -> anyhow::Result<()>;
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
    ) -> anyhow::Result<()> {
        let pubkey = encode_address(&account_write.pubkey);
        let owner = encode_address(&account_write.owner);

        // TODO: should update for same write_version to work with websocket input
        let query = postgres_query::query!(
            "INSERT INTO account_write
            (pubkey_id, slot, write_version,
             owner_id, lamports, executable, rent_epoch, data)
            VALUES
            (map_pubkey($pubkey), $slot, $write_version,
             map_pubkey($owner), $lamports, $executable, $rent_epoch, $data)
            ON CONFLICT (pubkey_id, slot, write_version) DO NOTHING",
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
