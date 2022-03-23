use {
    log::*,
    serde_derive::{Deserialize, Serialize},
    solana_geyser_connector_lib::chain_data::ChainData,
    solana_geyser_connector_lib::*,
    solana_sdk::pubkey::Pubkey,
    std::str::FromStr,
    std::{
        fs::File,
        io::Read,
        sync::{Arc, RwLock},
    },
};

use fixed::types::I80F48;
use mango::state::{DataType, MangoAccount, MangoCache, MangoGroup, MAX_PAIRS};
use solana_sdk::account::ReadableAccount;
use std::mem::size_of;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
}

type PnlData = Vec<(Pubkey, [I80F48; MAX_PAIRS])>;

fn compute_pnl(
    account: &MangoAccount,
    market_index: usize,
    group: &MangoGroup,
    cache: &MangoCache,
) -> I80F48 {
    let perp_account = &account.perp_accounts[market_index];
    let perp_market_cache = &cache.perp_market_cache[market_index];
    let price = cache.price_cache[market_index].price;
    let contract_size = group.perp_markets[market_index].base_lot_size;

    let base_pos = I80F48::from_num(perp_account.base_position * contract_size) * price;
    let quote_pos = perp_account.get_quote_position(&perp_market_cache);
    base_pos + quote_pos
}

// regularly updates pnl_data from chain_data
fn start_pnl_updater(chain_data: Arc<RwLock<ChainData>>, pnl_data: Arc<RwLock<PnlData>>) {
    let program_pk = Pubkey::from_str("mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68").unwrap();
    let group_pk = Pubkey::from_str("98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue").unwrap();
    let cache_pk = Pubkey::from_str("EBDRoayCDDUvDgCimta45ajQeXbexv7aKqJubruqpyvu").unwrap();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let snapshot = chain_data.read().unwrap().accounts_snapshot();

            // get the group and cache now
            let group = snapshot.get(&group_pk);
            let cache = snapshot.get(&cache_pk);
            if group.is_none() || cache.is_none() {
                continue;
            }
            let group: &MangoGroup = bytemuck::from_bytes(group.unwrap().account.data());
            let cache: &MangoCache = bytemuck::from_bytes(cache.unwrap().account.data());

            let mut pnls = Vec::with_capacity(snapshot.len());
            for (pubkey, account) in snapshot.iter() {
                let owner = account.account.owner();
                let data = account.account.data();
                if data.len() != size_of::<MangoAccount>()
                    || data[0] != DataType::MangoAccount as u8
                    || owner != &program_pk
                {
                    continue;
                }

                let mango_account: &MangoAccount = bytemuck::from_bytes(data);
                if mango_account.mango_group != group_pk {
                    continue;
                }

                let mut pnl_vals = [I80F48::ZERO; MAX_PAIRS];
                for market_index in 0..MAX_PAIRS {
                    pnl_vals[market_index] = compute_pnl(mango_account, market_index, group, cache);
                }

                // Alternatively, we could prepare the sorted and limited lists for each
                // market here. That would be faster and cause less contention on the pnl_data
                // lock, but it looks like it's very far from being an issue.
                pnls.push((pubkey.clone(), pnl_vals));
            }

            *pnl_data.write().unwrap() = pnls;
        }
    });
}

#[derive(Serialize, Deserialize, Debug)]
struct UnsettledPnlRankedRequest {
    market_index: u8,
    limit: u8,
    order: String,
}

#[derive(Serialize, Deserialize)]
struct PnlResponseItem {
    pnl: f64,
    pubkey: String,
}

type UnsettledPnlRankedResponse = Vec<PnlResponseItem>;

use jsonrpsee::http_server::HttpServerHandle;
fn start_jsonrpc_server(pnl_data: Arc<RwLock<PnlData>>) -> anyhow::Result<HttpServerHandle> {
    use jsonrpsee::core::Error;
    use jsonrpsee::http_server::{HttpServerBuilder, RpcModule};
    use jsonrpsee::types::error::CallError;
    use std::net::SocketAddr;

    let server = HttpServerBuilder::default().build("127.0.0.1:8889".parse::<SocketAddr>()?)?;
    let mut module = RpcModule::new(());
    module.register_method("unsettledPnlRanked", move |params, _| {
        let req = params.parse::<UnsettledPnlRankedRequest>()?;

        let invalid = |s| {
            Err(Error::Call(CallError::InvalidParams(anyhow::anyhow!(
                "'limit' must be <= 20"
            ))))
        };
        let limit = req.limit as usize;
        if limit > 20 {
            return invalid("'limit' must be <= 20");
        }
        let market_index = req.market_index as usize;
        if market_index >= MAX_PAIRS {
            return invalid("'market_index' must be < MAX_PAIRS");
        }
        if req.order != "ASC" && req.order != "DESC" {
            return invalid("'order' must be ASC or DESC");
        }

        // write lock, because we sort in-place...
        let mut pnls = pnl_data.write().unwrap();
        if req.order == "ASC" {
            pnls.sort_unstable_by(|a, b| a.1[market_index].cmp(&b.1[market_index]));
        } else {
            pnls.sort_unstable_by(|a, b| b.1[market_index].cmp(&a.1[market_index]));
        }
        let response = pnls
            .iter()
            .take(limit)
            .map(|p| PnlResponseItem {
                pnl: p.1[market_index].to_num::<f64>(),
                pubkey: p.0.to_string(),
            })
            .collect::<Vec<_>>();

        Ok(response)
    })?;

    Ok(server.start(module)?)
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
    let pnl_data = Arc::new(RwLock::new(PnlData::new()));

    start_pnl_updater(chain_data.clone(), pnl_data.clone());

    // dropping the handle would exit the server
    let http_server_handle = start_jsonrpc_server(pnl_data)?;

    // start filling chain_data from the grpc plugin source
    let (account_write_queue_sender, slot_queue_sender) = memory_target::init(chain_data).await?;
    grpc_plugin_source::process_events(
        &config.source,
        account_write_queue_sender,
        slot_queue_sender,
        metrics_tx,
    )
    .await;

    Ok(())
}
