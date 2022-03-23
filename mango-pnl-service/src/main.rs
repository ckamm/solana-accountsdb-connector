use {
    log::*,
    serde_derive::Deserialize,
    solana_geyser_connector_lib::chain_data::ChainData,
    solana_geyser_connector_lib::*,
    std::{
        fs::File,
        io::Read,
        sync::{Arc, RwLock},
    },
};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("requires a config file argument");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    solana_logger::setup_with_default("info");
    info!("startup");

    let metrics_tx = metrics::start();

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    let (account_write_queue_sender, slot_queue_sender) =
        memory_target::init(chain_data.clone()).await?;
    grpc_plugin_source::process_events(
        &config.source,
        account_write_queue_sender,
        slot_queue_sender,
        metrics_tx,
    )
    .await;

    Ok(())
}
