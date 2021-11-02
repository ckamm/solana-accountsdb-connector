mod grpc_plugin_source;
mod postgres_target;
mod websocket_source;

use solana_sdk::pubkey::Pubkey;

use log::*;

struct SimpleLogger;
impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}
static LOGGER: SimpleLogger = SimpleLogger;

//
// main etc.
//

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
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
        .unwrap();
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
