use {
    log::*,
    serde_derive::Deserialize,
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

type PnlData = Vec<(Pubkey, [I80F48; MAX_PAIRS])>;

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

    let program_pk = Pubkey::from_str("mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68").unwrap();
    let group_pk = Pubkey::from_str("98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue").unwrap();
    let cache_pk = Pubkey::from_str("EBDRoayCDDUvDgCimta45ajQeXbexv7aKqJubruqpyvu").unwrap();

    let chain_data = Arc::new(RwLock::new(ChainData::new()));
    let pnl_data = Arc::new(RwLock::new(PnlData::new()));

    let chain_data_c = chain_data.clone();
    let pnl_data_c = pnl_data.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let snapshot = chain_data_c.read().unwrap().accounts_snapshot();

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
                pnls.push((pubkey.clone(), pnl_vals));
            }
            for market_index in 0..MAX_PAIRS {
                pnls.sort_unstable_by(|a, b| a.1[market_index].cmp(&b.1[market_index]));
            }

            *pnl_data_c.write().unwrap() = pnls;
        }
    });

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
