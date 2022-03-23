use anyhow::Context;
use log::*;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use postgres_query::{query, query_dyn};
use std::{collections::HashMap, convert::TryFrom, time::Duration};

use crate::{metrics, AccountTables, AccountWrite, PostgresConfig, SlotStatus, SlotUpdate};

mod pg {
    #[derive(Clone, Copy, Debug, PartialEq, postgres_types::ToSql)]
    pub enum SlotStatus {
        Rooted,
        Confirmed,
        Processed,
    }

    impl From<super::SlotStatus> for SlotStatus {
        fn from(status: super::SlotStatus) -> SlotStatus {
            match status {
                super::SlotStatus::Rooted => SlotStatus::Rooted,
                super::SlotStatus::Confirmed => SlotStatus::Confirmed,
                super::SlotStatus::Processed => SlotStatus::Processed,
            }
        }
    }
}

async fn postgres_connection(
    config: &PostgresConfig,
    metric_retries: metrics::MetricU64,
    metric_live: metrics::MetricU64,
) -> anyhow::Result<async_channel::Receiver<Option<tokio_postgres::Client>>> {
    let (tx, rx) = async_channel::unbounded();

    let tls = MakeTlsConnector::new(
        TlsConnector::builder()
            .danger_accept_invalid_certs(config.allow_invalid_certs)
            .build()?,
    );

    let config = config.clone();
    let mut initial = Some(tokio_postgres::connect(&config.connection_string, tls.clone()).await?);
    let mut metric_retries = metric_retries;
    let mut metric_live = metric_live;
    tokio::spawn(async move {
        loop {
            let (client, connection) = match initial.take() {
                Some(v) => v,
                None => {
                    let result =
                        tokio_postgres::connect(&config.connection_string, tls.clone()).await;
                    match result {
                        Ok(v) => v,
                        Err(err) => {
                            warn!("could not connect to postgres: {:?}", err);
                            tokio::time::sleep(Duration::from_secs(
                                config.retry_connection_sleep_secs,
                            ))
                            .await;
                            continue;
                        }
                    }
                }
            };

            tx.send(Some(client)).await.expect("send success");
            metric_live.increment();

            let result = connection.await;

            metric_retries.increment();
            metric_live.decrement();

            tx.send(None).await.expect("send success");
            warn!("postgres connection error: {:?}", result);
            tokio::time::sleep(Duration::from_secs(config.retry_connection_sleep_secs)).await;
        }
    });

    Ok(rx)
}

async fn update_postgres_client<'a>(
    client: &'a mut Option<postgres_query::Caching<tokio_postgres::Client>>,
    rx: &async_channel::Receiver<Option<tokio_postgres::Client>>,
    config: &PostgresConfig,
) -> &'a postgres_query::Caching<tokio_postgres::Client> {
    // get the most recent client, waiting if there's a disconnect
    while !rx.is_empty() || client.is_none() {
        tokio::select! {
            client_raw_opt = rx.recv() => {
                *client = client_raw_opt.expect("not closed").map(postgres_query::Caching::new);
            },
            _ = tokio::time::sleep(Duration::from_secs(config.fatal_connection_timeout_secs)) => {
                error!("waited too long for new postgres client");
                std::process::exit(1);
            },
        }
    }
    client.as_ref().expect("must contain value")
}

async fn process_account_write(
    client: &postgres_query::Caching<tokio_postgres::Client>,
    write: &AccountWrite,
    account_tables: &AccountTables,
) -> anyhow::Result<()> {
    futures::future::try_join_all(
        account_tables
            .iter()
            .map(|table| table.insert_account_write(client, write)),
    )
    .await?;
    Ok(())
}

struct Slots {
    // non-rooted only
    slots: HashMap<u64, SlotUpdate>,
    newest_processed_slot: Option<u64>,
    newest_rooted_slot: Option<u64>,
}

#[derive(Default)]
struct SlotPreprocessing {
    discard_duplicate: bool,
    discard_old: bool,
    new_processed_head: bool,
    new_rooted_head: bool,
    parent_update: bool,
}

impl Slots {
    fn new() -> Self {
        Self {
            slots: HashMap::new(),
            newest_processed_slot: None,
            newest_rooted_slot: None,
        }
    }

    fn add(&mut self, update: &SlotUpdate) -> SlotPreprocessing {
        let mut result = SlotPreprocessing::default();

        if let Some(previous) = self.slots.get_mut(&update.slot) {
            if previous.status == update.status && previous.parent == update.parent {
                result.discard_duplicate = true;
            }

            previous.status = update.status;
            if update.parent.is_some() && previous.parent != update.parent {
                previous.parent = update.parent;
                result.parent_update = true;
            }
        } else if self.newest_rooted_slot.is_none()
            || update.slot > self.newest_rooted_slot.unwrap()
        {
            self.slots.insert(update.slot, update.clone());
        } else {
            result.discard_old = true;
        }

        if update.status == SlotStatus::Rooted {
            let old_slots: Vec<u64> = self
                .slots
                .keys()
                .filter(|s| **s <= update.slot)
                .copied()
                .collect();
            for old_slot in old_slots {
                self.slots.remove(&old_slot);
            }
            if self.newest_rooted_slot.is_none() || self.newest_rooted_slot.unwrap() < update.slot {
                self.newest_rooted_slot = Some(update.slot);
                result.new_rooted_head = true;
            }
        }

        if self.newest_processed_slot.is_none() || self.newest_processed_slot.unwrap() < update.slot
        {
            self.newest_processed_slot = Some(update.slot);
            result.new_processed_head = true;
        }

        result
    }
}

fn make_cleanup_steps(tables: &Vec<String>) -> HashMap<String, String> {
    let mut steps = HashMap::<String, String>::new();

    // Delete all account writes that came before the newest rooted slot except
    // for the newest rooted write for each pubkey.
    // This could be older rooted writes or writes in uncled slots that came
    // before the newest rooted slot.
    //
    // Also delete _all_ writes from before the newest snapshot, because these may
    // be for deleted accounts where the deletion event was missed. Snapshots
    // provide a new state for all live accounts, but don't tell us about deleted
    // accounts.
    //
    // The way this is done, by taking the newest snapshot that's at least
    // min_snapshot_age behind the newest rooted slot is a workaround: we don't know
    // how long it'll take to insert snapshot data, but assume it'll be done by that
    // time.
    let min_snapshot_age = 300;
    steps.extend(
        tables
            .iter()
            .map(|table_name| {
                let sql = format!(
                    "WITH
                    newest_rooted AS (
                        SELECT max(slot) AS newest_rooted_slot FROM slot WHERE status = 'Rooted'),
                    newest_snapshot AS (
                        SELECT max(slot) AS newest_snapshot_slot FROM account_write, newest_rooted
                        WHERE write_version = 0 AND slot + {min_snapshot_age} < newest_rooted_slot)
                DELETE FROM {table} AS data
                USING
                    newest_rooted,
                    newest_snapshot,
                    (SELECT DISTINCT ON(pubkey_id) pubkey_id, slot, write_version
                     FROM {table}
                     LEFT JOIN slot USING(slot)
                     CROSS JOIN newest_rooted
                     WHERE slot <= newest_rooted_slot AND (status = 'Rooted' OR status is NULL)
                     ORDER BY pubkey_id, slot DESC, write_version DESC
                     ) newest_rooted_write
                WHERE
                    data.pubkey_id = newest_rooted_write.pubkey_id AND (
                        data.slot < newest_snapshot_slot OR (
                            data.slot <= newest_rooted_slot
                            AND (data.slot != newest_rooted_write.slot OR data.write_version != newest_rooted_write.write_version)
                        )
                    )",
                    table = table_name,
                    min_snapshot_age = min_snapshot_age,
                );
                (format!("delete old writes in {}", table_name), sql)
            })
            .collect::<HashMap<String, String>>(),
    );

    // Delete information about older slots
    steps.insert(
        "delete old slots".into(),
        "DELETE FROM slot
         USING (SELECT max(slot) as newest_rooted_slot FROM slot WHERE status = 'Rooted') s
         WHERE slot + 1000 < newest_rooted_slot"
            .into(),
    );

    steps
}

#[derive(Clone)]
struct SlotsProcessing {}

impl SlotsProcessing {
    fn new() -> Self {
        Self {}
    }

    async fn process(
        &self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        update: &SlotUpdate,
        meta: &SlotPreprocessing,
    ) -> anyhow::Result<()> {
        let slot = update.slot as i64;
        let status: pg::SlotStatus = update.status.into();
        if let Some(parent) = update.parent {
            let parent = parent as i64;
            let query = query!(
                "INSERT INTO slot
                    (slot, parent, status, uncle)
                VALUES
                    ($slot, $parent, $status, FALSE)
                ON CONFLICT (slot) DO UPDATE SET
                    parent=$parent, status=$status",
                slot,
                parent,
                status,
            );
            let _ = query.execute(client).await.context("updating slot row")?;
        } else {
            let query = query!(
                "INSERT INTO slot
                    (slot, parent, status, uncle)
                VALUES
                    ($slot, NULL, $status, FALSE)
                ON CONFLICT (slot) DO UPDATE SET
                    status=$status",
                slot,
                status,
            );
            let _ = query.execute(client).await.context("updating slot row")?;
        }

        if meta.new_rooted_head {
            let slot = update.slot as i64;
            // Mark preceeding non-uncle slots as rooted
            let query = query!(
                "UPDATE slot SET status = 'Rooted'
                WHERE slot < $newest_final_slot
                AND (NOT uncle)
                AND status != 'Rooted'",
                newest_final_slot = slot
            );
            let _ = query
                .execute(client)
                .await
                .context("updating preceding non-rooted slots")?;
        }

        if meta.new_processed_head || meta.parent_update {
            // update the uncle column for the chain of slots from the
            // newest down the the first rooted slot
            let query = query!(
                "WITH RECURSIVE
                    liveslots AS (
                        SELECT slot.*, 0 AS depth FROM slot
                            WHERE slot = (SELECT max(slot) FROM slot)
                        UNION ALL
                        SELECT s.*, depth + 1 FROM slot s
                            INNER JOIN liveslots l ON s.slot = l.parent
                            WHERE l.status != 'Rooted' AND depth < 1000
                    ),
                    min_slot AS (SELECT min(slot) AS min_slot FROM liveslots)
                UPDATE slot SET
                    uncle = NOT EXISTS (SELECT 1 FROM liveslots WHERE liveslots.slot = slot.slot)
                    FROM min_slot
                    WHERE slot >= min_slot;"
            );
            let _ = query
                .execute(client)
                .await
                .context("recomputing slot uncle status")?;
        }

        trace!("slot update done {}", update.slot);
        Ok(())
    }
}

fn secs_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
fn epoch_secs_to_time(secs: u64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)
}

pub async fn init(
    config: &PostgresConfig,
    account_tables: AccountTables,
    metrics_sender: metrics::Metrics,
) -> anyhow::Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Sender<SlotUpdate>,
)> {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::bounded::<AccountWrite>(config.account_write_max_queue_size);

    // Slot updates flowing from the outside into the single processing thread. From
    // there they'll flow into the postgres sending thread.
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();
    let (slot_inserter_sender, slot_inserter_receiver) =
        async_channel::unbounded::<(SlotUpdate, SlotPreprocessing)>();

    let metric_con_retries = metrics_sender.register_u64("postgres_connection_retries".into());
    let metric_con_live = metrics_sender.register_u64("postgres_connections_alive".into());

    // postgres account write sending worker threads
    for _ in 0..config.account_write_connection_count {
        let postgres_account_writes =
            postgres_connection(config, metric_con_retries.clone(), metric_con_live.clone())
                .await?;
        let account_write_queue_receiver_c = account_write_queue_receiver.clone();
        let account_tables_c = account_tables.clone();
        let config = config.clone();
        let mut metric_retries =
            metrics_sender.register_u64("postgres_account_write_retries".into());
        let mut metric_last_write =
            metrics_sender.register_u64("postgres_account_write_last_write_timestamp".into());
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                // Retrieve up to batch_size account writes
                let mut write_batch = Vec::new();
                write_batch.push(
                    account_write_queue_receiver_c
                        .recv()
                        .await
                        .expect("sender must stay alive"),
                );
                while write_batch.len() < config.account_write_max_batch_size {
                    match account_write_queue_receiver_c.try_recv() {
                        Ok(write) => write_batch.push(write),
                        Err(async_channel::TryRecvError::Empty) => break,
                        Err(async_channel::TryRecvError::Closed) => {
                            panic!("sender must stay alive")
                        }
                    };
                }

                trace!(
                    "account write, batch {}, channel size {}",
                    write_batch.len(),
                    account_write_queue_receiver_c.len(),
                );

                let mut error_count = 0;
                loop {
                    let client =
                        update_postgres_client(&mut client_opt, &postgres_account_writes, &config)
                            .await;
                    let mut results = futures::future::join_all(
                        write_batch
                            .iter()
                            .map(|write| process_account_write(client, &write, &account_tables_c)),
                    )
                    .await;
                    let mut iter = results.iter();
                    write_batch.retain(|_| iter.next().unwrap().is_err());
                    if write_batch.len() > 0 {
                        metric_retries.add(write_batch.len() as u64);
                        error_count += 1;
                        if error_count - 1 < config.retry_query_max_count {
                            results.retain(|r| r.is_err());
                            warn!("failed to process account write, retrying: {:?}", results);
                            tokio::time::sleep(Duration::from_secs(config.retry_query_sleep_secs))
                                .await;
                            continue;
                        } else {
                            error!("failed to process account write, exiting");
                            std::process::exit(1);
                        }
                    };
                    break;
                }
                metric_last_write.set_max(secs_since_epoch());
            }
        });
    }

    // slot update handling thread
    let mut metric_slot_queue = metrics_sender.register_u64("slot_insert_queue".into());
    tokio::spawn(async move {
        let mut slots = Slots::new();

        loop {
            let update = slot_queue_receiver
                .recv()
                .await
                .expect("sender must stay alive");
            trace!(
                "slot update {}, channel size {}",
                update.slot,
                slot_queue_receiver.len()
            );

            // Check if we already know about the slot, or it is outdated
            let slot_preprocessing = slots.add(&update);
            if slot_preprocessing.discard_duplicate || slot_preprocessing.discard_old {
                continue;
            }

            slot_inserter_sender
                .send((update, slot_preprocessing))
                .await
                .expect("sending must succeed");
            metric_slot_queue.set(slot_inserter_sender.len() as u64);
        }
    });

    // postgres slot update worker threads
    let slots_processing = SlotsProcessing::new();
    for _ in 0..config.slot_update_connection_count {
        let postgres_slot =
            postgres_connection(config, metric_con_retries.clone(), metric_con_live.clone())
                .await?;
        let receiver_c = slot_inserter_receiver.clone();
        let config = config.clone();
        let mut metric_retries = metrics_sender.register_u64("postgres_slot_update_retries".into());
        let mut metric_last_write =
            metrics_sender.register_u64("postgres_slot_last_write_timestamp".into());
        let slots_processing = slots_processing.clone();
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                let (update, preprocessing) =
                    receiver_c.recv().await.expect("sender must stay alive");
                trace!("slot insertion, slot {}", update.slot);

                let mut error_count = 0;
                loop {
                    let client =
                        update_postgres_client(&mut client_opt, &postgres_slot, &config).await;
                    if let Err(err) = slots_processing
                        .process(client, &update, &preprocessing)
                        .await
                    {
                        metric_retries.increment();
                        error_count += 1;
                        if error_count - 1 < config.retry_query_max_count {
                            warn!("failed to process slot update, retrying: {:?}", err);
                            tokio::time::sleep(Duration::from_secs(config.retry_query_sleep_secs))
                                .await;
                            continue;
                        } else {
                            error!("failed to process slot update, exiting");
                            std::process::exit(1);
                        }
                    };
                    break;
                }
                metric_last_write.set_max(secs_since_epoch());
            }
        });
    }

    // postgres cleanup thread
    if config.cleanup_interval_secs > 0 {
        let table_names: Vec<String> = account_tables
            .iter()
            .map(|table| table.table_name().to_string())
            .collect();
        let cleanup_steps = make_cleanup_steps(&table_names);

        let postgres_con =
            postgres_connection(config, metric_con_retries.clone(), metric_con_live.clone())
                .await?;
        let mut metric_last_cleanup =
            metrics_sender.register_u64("postgres_cleanup_last_success_timestamp".into());
        let mut metric_cleanup_errors =
            metrics_sender.register_u64("postgres_cleanup_errors".into());
        let config = config.clone();
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                tokio::time::sleep(Duration::from_secs(config.cleanup_interval_secs)).await;
                let client = update_postgres_client(&mut client_opt, &postgres_con, &config).await;

                let mut all_successful = true;
                for (name, cleanup_sql) in &cleanup_steps {
                    let query = query_dyn!(&cleanup_sql).unwrap();
                    if let Err(err) = query.execute(client).await {
                        warn!("failed to process cleanup step {}: {:?}", name, err);
                        metric_cleanup_errors.increment();
                        all_successful = false;
                    }
                }
                if all_successful {
                    metric_last_cleanup.set_max(secs_since_epoch());
                }
            }
        });
    }

    // postgres metrics/monitoring thread
    {
        let postgres_con =
            postgres_connection(config, metric_con_retries.clone(), metric_con_live.clone())
                .await?;
        let metric_slot_last_write =
            metrics_sender.register_u64("postgres_slot_last_write_timestamp".into());
        let metric_account_write_last_write =
            metrics_sender.register_u64("postgres_account_write_last_write_timestamp".into());
        let metric_account_queue = metrics_sender.register_u64("account_write_queue".into());
        let metric_slot_queue = metrics_sender.register_u64("slot_insert_queue".into());
        let config = config.clone();
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                tokio::time::sleep(Duration::from_secs(config.monitoring_update_interval_secs))
                    .await;

                let client = update_postgres_client(&mut client_opt, &postgres_con, &config).await;
                let last_update = std::time::SystemTime::now();
                let last_slot_write = epoch_secs_to_time(metric_slot_last_write.value());
                let last_account_write_write =
                    epoch_secs_to_time(metric_account_write_last_write.value());
                let slot_queue = i64::try_from(metric_slot_queue.value()).unwrap();
                let account_write_queue = i64::try_from(metric_account_queue.value()).unwrap();
                let query = query!(
                    "INSERT INTO monitoring
                        (name, last_update, last_slot_write, last_account_write_write, slot_queue, account_write_queue)
                    VALUES
                        ($name, $last_update, $last_slot_write, $last_account_write_write, $slot_queue, $account_write_queue)
                    ON CONFLICT (name) DO UPDATE SET
                        last_update=$last_update,
                        last_slot_write=$last_slot_write,
                        last_account_write_write=$last_account_write_write,
                        slot_queue=$slot_queue,
                        account_write_queue=$account_write_queue
                    ",
                    name = config.monitoring_name,
                    last_update,
                    last_slot_write,
                    last_account_write_write,
                    slot_queue,
                    account_write_queue,
                );
                if let Err(err) = query
                    .execute(client)
                    .await
                    .context("updating monitoring table")
                {
                    warn!("failed to process monitoring update: {:?}", err);
                };
            }
        });
    }

    Ok((account_write_queue_sender, slot_queue_sender))
}
