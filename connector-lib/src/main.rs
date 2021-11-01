mod grpc_plugin_source;
mod websocket_source;

//use solana_program::account_info::AccountInfo;
//use mango::state::{MangoAccount, MangoCache, MangoGroup};

use solana_sdk::pubkey::Pubkey;

use log::{error, info, trace, warn};
use postgres_query::{query, FromSqlRow};
use std::{
    cmp::max,
    collections::HashMap,
    ops::Deref,
    sync::{mpsc, Arc, RwLock},
    time::{Duration, Instant},
};

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

fn init_postgres(
    connection_string: String,
) -> (mpsc::Sender<AccountWrite>, mpsc::Sender<SlotUpdate>) {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        mpsc::channel::<AccountWrite>();

    // slot updates are not parallel because their order matters
    let (slot_queue_sender, slot_queue_receiver) = mpsc::channel::<SlotUpdate>();

    // the postgres connection management thread
    // - creates a connection and runs it
    // - if it fails, reestablishes it and requests a new snapshot
    let postgres_client: Arc<RwLock<Arc<Option<postgres_query::Caching<tokio_postgres::Client>>>>> =
        Arc::new(RwLock::new(Arc::new(None)));
    let postgres_client_c = Arc::clone(&postgres_client);
    let connection_string_c = connection_string.clone();
    tokio::spawn(async move {
        let (client, connection) =
            tokio_postgres::connect(&connection_string_c, tokio_postgres::NoTls)
                .await
                .unwrap();
        {
            let mut w = postgres_client_c.write().unwrap();
            *w = Arc::new(Some(postgres_query::Caching::new(client)));
        }
        connection.await.unwrap();
        // TODO: on error: log, reconnect, send message that a new snapshot is necessary
    });

    // separate postgres client for slot updates, because they need transactions
    let postgres_client_slots: Arc<
        RwLock<Option<postgres_query::Caching<tokio_postgres::Client>>>,
    > = Arc::new(RwLock::new(None));
    let postgres_client_slots_c = Arc::clone(&postgres_client_slots);
    let connection_string_c = connection_string.clone();
    tokio::spawn(async move {
        let (client, connection) =
            tokio_postgres::connect(&connection_string_c, tokio_postgres::NoTls)
                .await
                .unwrap();
        {
            let mut w = postgres_client_slots_c.write().unwrap();
            *w = Some(postgres_query::Caching::new(client));
        }
        connection.await.unwrap();
        // TODO: on error: log, reconnect, send message that a new snapshot is necessary
    });

    #[derive(FromSqlRow)]
    struct SingleResult(i64);

    // postgres account write sending worker thread
    let postgres_client_c = Arc::clone(&postgres_client);
    tokio::spawn(async move {
        loop {
            // all of this needs to be in a function, to allow ?
            let write = account_write_queue_receiver.recv().unwrap();

            // copy the client arc
            let client_opt = Arc::clone(&*postgres_client_c.read().unwrap());

            // Not sure this is right. What I want is a single thread that calls
            // query.fetch(client) and then to process the results of that
            // in a bunch of worker threads.
            // However, the future returned from query.fetch(client) still requires
            // client, so that isn't possible.
            // TODO: Nevertheless, there should just be a limited number of these processing threads. Maybe have an intermediary that the worker threads
            // send their sender to, then receive work through it?
            tokio::spawn(async move {
                let client = client_opt.deref().as_ref().unwrap();

                let pubkey: &[u8] = &write.pubkey.to_bytes();
                let owner: &[u8] = &write.owner.to_bytes();

                let query = query!(" \
                    INSERT INTO account_write \
                    (pubkey, slot, write_version, owner, lamports, executable, rent_epoch, data) \
                    VALUES \
                    ($pubkey, $slot, $write_version, $owner, $lamports, $executable, $rent_epoch, $data) \
                    ON CONFLICT (pubkey, slot, write_version) DO NOTHING", // TODO: should update for same write_version to work with websocket input
                    pubkey,
                    slot = write.slot,
                    write_version = write.write_version,
                    owner,
                    lamports = write.lamports,
                    executable = write.executable,
                    rent_epoch = write.rent_epoch,
                    data = write.data,
                    );
                let result = query.execute(client).await.unwrap();
                println!("new write: count: {}", result);
            });
        }
    });

    // slot update handling thread
    let postgres_client_slots_c = Arc::clone(&postgres_client_slots);
    tokio::spawn(async move {
        let mut slots = HashMap::<i64, SlotUpdate>::new();
        let mut newest_nonfinal_slot: Option<i64> = None;
        let mut newest_final_slot: Option<i64> = None;

        let mut client: Option<postgres_query::Caching<tokio_postgres::Client>> = None;

        loop {
            let update = slot_queue_receiver.recv().unwrap();

            // since we need to mutate the client, move it out of the rwlock here
            // TODO: might be easier to understand if we sent the new client over a channel instead
            {
                let mut lock = postgres_client_slots_c.write().unwrap();
                if (*lock).is_some() {
                    client = (*lock).take();
                }
            }
            let client = client.as_mut().unwrap();

            if let Some(parent) = update.parent {
                let query = query!(
                    " \
                    INSERT INTO slot \
                        (slot, parent, status, uncle) \
                    VALUES \
                        ($slot, $parent, $status, FALSE) \
                    ON CONFLICT (slot) DO UPDATE SET \
                        parent=$parent, status=$status",
                    slot = update.slot,
                    parent = parent,
                    status = update.status,
                );
                let result = query.execute(client).await.unwrap();
                println!("new slot: count: {}", result);
            } else {
                let query = query!(
                    " \
                    INSERT INTO slot \
                        (slot, parent, status, uncle) \
                    VALUES \
                        ($slot, NULL, $status, FALSE) \
                    ON CONFLICT (slot) DO UPDATE SET \
                        status=$status",
                    slot = update.slot,
                    status = update.status,
                );
                let result = query.execute(client).await.unwrap();
                println!("new slot: count: {}", result);
            }

            if update.status == "rooted" {
                println!("slot changed to rooted");
                slots.remove(&update.slot);

                // TODO: should also convert all parents to rooted, just in case we missed an update

                // Keep only the most recent final write per pubkey
                if newest_final_slot.unwrap_or(-1) < update.slot {
                    let query = query!(
                        " \
                        DELETE FROM account_write \
                        USING ( \
                            SELECT DISTINCT ON(pubkey) pubkey, slot, write_version \
                            FROM account_write \
                            INNER JOIN slot USING(slot) \
                            WHERE slot <= $newest_final_slot AND status = 'rooted' \
                            ORDER BY pubkey, slot DESC, write_version DESC \
                            ) latest_write \
                        WHERE account_write.pubkey = latest_write.pubkey \
                        AND (account_write.slot < latest_write.slot \
                            OR (account_write.slot = latest_write.slot \
                                AND account_write.write_version < latest_write.write_version \
                            ) \
                        )",
                        newest_final_slot = update.slot,
                    );
                    let result = query.execute(client).await.unwrap();

                    newest_final_slot = Some(update.slot);
                }

                if newest_nonfinal_slot.unwrap_or(-1) == update.slot {
                    newest_nonfinal_slot = None;
                }
            } else {
                let mut parent_update = false;
                if let Some(previous) = slots.get_mut(&update.slot) {
                    previous.status = update.status;
                    if update.parent.is_some() {
                        previous.parent = update.parent;
                        parent_update = true;
                    }
                } else {
                    slots.insert(update.slot, update.clone());
                }

                let new_newest_slot = newest_nonfinal_slot.unwrap_or(-1) < update.slot;

                if new_newest_slot || parent_update {
                    println!("recomputing uncles");
                    // recompute uncles and store to postgres for each slot in slots
                    let mut uncles: HashMap<i64, bool> = slots.keys().map(|k| (*k, true)).collect();

                    /* Could do this in SQL like...
                    with recursive liveslots as (
                        select * from slot where slot = (select max(slot) from slot)
                        union
                        select s.* from slot s inner join liveslots l on l.parent = s.slot where s.status != 'rooted'
                    )
                    select * from liveslots order by slot;
                    */
                    let mut it_slot = max(newest_nonfinal_slot.unwrap_or(-1), update.slot);
                    while let Some(slot) = slots.get(&it_slot) {
                        let w = uncles
                            .get_mut(&it_slot)
                            .expect("uncles has same keys as slots");
                        assert!(*w); // TODO: error instead
                        *w = false;
                        if slot.status == "rooted" {
                            break;
                        }
                        if let Some(parent) = slot.parent {
                            it_slot = parent;
                        } else {
                            break;
                        }
                    }

                    // Updating uncle state must be done as a transaction
                    let transaction = client.transaction().await.unwrap();
                    for (slot, is_uncle) in uncles {
                        let query = query!(
                            "UPDATE slot SET uncle = $is_uncle WHERE slot = $slot",
                            is_uncle,
                            slot,
                        );
                        let result = query.execute(&transaction).await.unwrap();
                    }
                    transaction.into_inner().commit().await.unwrap();
                }

                if new_newest_slot {
                    newest_nonfinal_slot = Some(update.slot);
                }
            }
        }
    });

    (account_write_queue_sender, slot_queue_sender)
}

#[tokio::main]
async fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
        .unwrap();

    let postgres_connection_string = "host=/var/run/postgresql user=kamm port=5433";
    let (account_write_queue_sender, slot_queue_sender) =
        init_postgres(postgres_connection_string.into());

    let use_accountsdb = true;

    if use_accountsdb {
        grpc_plugin_source::process_events(account_write_queue_sender, slot_queue_sender);
    } else {
        websocket_source::process_events(account_write_queue_sender, slot_queue_sender);
    }
}
