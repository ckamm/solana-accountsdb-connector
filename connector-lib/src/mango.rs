mod grpc_plugin_source;
mod websocket_source;

//use solana_program::account_info::AccountInfo;
//use mango::state::{MangoAccount, MangoCache, MangoGroup};

use solana_sdk::pubkey::Pubkey;

use log::{error, info, trace, warn};
use std::{
    cmp::max,
    collections::HashMap,
    ops::Deref,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/*
pub struct MangoData {
    group: Option<MangoGroup>,
    cache: Option<MangoCache>,
}
impl MangoData {
    fn new() -> Self {
        Self {
            group: None,
            cache: None,
        }
    }
}

//
// updating mango data
//
async fn get_account(client: &FullClient, pubkey: String) -> Option<Account> {
    client
        .get_account_info(
            pubkey,
            Some(RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::processed()),
                data_slice: None,
            }),
        )
        .await
        .ok()?
        .value?
        .decode()
}

async fn get_mango_group(
    client: &FullClient,
    group_key: &Pubkey,
    program_id: &Pubkey,
) -> Option<MangoGroup> {
    let mut account = get_account(client, group_key.to_string()).await?;
    let accountInfo = AccountInfo::from((group_key, &mut account));
    let group = MangoGroup::load_checked(&accountInfo, program_id)
        .ok()?
        .deref()
        .clone();
    Some(group)
}

async fn get_mango_cache(
    client: &FullClient,
    group: &MangoGroup,
    program_id: &Pubkey,
) -> Option<MangoCache> {
    let mut account = get_account(client, group.mango_cache.to_string()).await?;
    let accountInfo = AccountInfo::from((&group.mango_cache, &mut account));
    let cache = MangoCache::load_checked(&accountInfo, program_id, group)
        .ok()?
        .deref()
        .clone();
    Some(cache)
}

async fn update_mango_data(mango_data: Arc<RwLock<MangoData>>) {
    let program_id =
        Pubkey::from_str("mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68").expect("valid pubkey");
    let mango_group_address =
        Pubkey::from_str("98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue").expect("valid pubkey");

    let rpc_http_url = "";
    let rpc_client = http::connect_with_options::<FullClient>(&rpc_http_url, true)
        .await
        .unwrap();

    loop {
        let group = get_mango_group(&rpc_client, &mango_group_address, &program_id)
            .await
            .unwrap();
        let cache = get_mango_cache(&rpc_client, &group, &program_id)
            .await
            .unwrap();

        {
            let mut writer = mango_data.write().unwrap();
            writer.group = Some(group);
            writer.cache = Some(cache);
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}*/
