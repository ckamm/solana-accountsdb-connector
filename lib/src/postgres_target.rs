use anyhow::Context;
use log::*;
use postgres_query::{query, query_dyn};
use std::{collections::HashMap, time::Duration};

use crate::{metrics, AccountTables, AccountWrite, PostgresConfig, SlotStatus, SlotUpdate};

async fn postgres_connection(
    config: &PostgresConfig,
    metric_retries: metrics::MetricU64,
    metric_live: metrics::MetricU64,
) -> anyhow::Result<async_channel::Receiver<Option<tokio_postgres::Client>>> {
    let (tx, rx) = async_channel::unbounded();

    let config = config.clone();
    let mut initial =
        Some(tokio_postgres::connect(&config.connection_string, tokio_postgres::NoTls).await?);
    let mut metric_retries = metric_retries;
    let mut metric_live = metric_live;
    tokio::spawn(async move {
        loop {
            let (client, connection) = match initial.take() {
                Some(v) => v,
                None => {
                    let result =
                        tokio_postgres::connect(&config.connection_string, tokio_postgres::NoTls)
                            .await;
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
                *client = client_raw_opt.expect("not closed").map(|client| postgres_query::Caching::new(client));
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
    for account_table in account_tables {
        // TODO: Could run all these in parallel instead of sequentially
        let _ = account_table.insert_account_write(client, write).await?;
    }

    Ok(())
}

struct SlotsProcessing {
    slots: HashMap<i64, SlotUpdate>,
    newest_nonfinal_slot: Option<i64>,
    newest_final_slot: Option<i64>,
    cleanup_table_sql: Vec<String>,
    metric_update_rooted: metrics::MetricU64,
    metric_update_uncles: metrics::MetricU64,
}

impl SlotsProcessing {
    fn set_cleanup_tables(&mut self, tables: &Vec<String>) {
        self.cleanup_table_sql = tables
            .iter()
            .map(|table_name| {
                format!(
                    "DELETE FROM {table} AS data
                    USING (
                        SELECT DISTINCT ON(pubkey_id) pubkey_id, slot, write_version
                        FROM {table}
                        INNER JOIN slot USING(slot)
                        WHERE slot <= $newest_final_slot AND status = 'Rooted'
                        ORDER BY pubkey_id, slot DESC, write_version DESC
                        ) latest_write
                    WHERE data.pubkey_id = latest_write.pubkey_id
                    AND (data.slot < latest_write.slot
                        OR (data.slot = latest_write.slot
                            AND data.write_version < latest_write.write_version
                        )
                    )",
                    table = table_name
                )
            })
            .collect();

        self.cleanup_table_sql
            .push("DELETE FROM slot WHERE slot + 100000 < $newest_final_slot".into());
    }

    async fn process(
        &mut self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        update: &SlotUpdate,
    ) -> anyhow::Result<()> {
        if let Some(parent) = update.parent {
            let query = query!(
                "INSERT INTO slot
                    (slot, parent, status, uncle)
                VALUES
                    ($slot, $parent, $status, FALSE)
                ON CONFLICT (slot) DO UPDATE SET
                    parent=$parent, status=$status",
                slot = update.slot,
                parent = parent,
                status = update.status,
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
                slot = update.slot,
                status = update.status,
            );
            let _ = query.execute(client).await.context("updating slot row")?;
        }

        if update.status == SlotStatus::Rooted {
            self.metric_update_rooted.increment();
            self.slots.remove(&update.slot);

            // TODO: should also convert all parents to rooted, just in case we missed an update?

            // Keep only the most recent final write per pubkey
            if self.newest_final_slot.unwrap_or(-1) < update.slot {
                // Keep only the newest rooted account write and also
                // wipe old slots
                for cleanup_sql in &self.cleanup_table_sql {
                    let query = query_dyn!(cleanup_sql, newest_final_slot = update.slot)?;
                    let _ = query
                        .execute(client)
                        .await
                        .context("deleting old account writes")?;
                }

                self.newest_final_slot = Some(update.slot);
            }

            if self.newest_nonfinal_slot.unwrap_or(-1) == update.slot {
                self.newest_nonfinal_slot = None;
            }
        } else {
            let mut parent_update = false;
            if let Some(previous) = self.slots.get_mut(&update.slot) {
                previous.status = update.status.clone();
                if update.parent.is_some() {
                    previous.parent = update.parent;
                    parent_update = true;
                }
            } else {
                self.slots.insert(update.slot, update.clone());
            }

            let new_newest_slot = self.newest_nonfinal_slot.unwrap_or(-1) < update.slot;

            if new_newest_slot || parent_update {
                self.metric_update_uncles.increment();
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

            if new_newest_slot {
                self.newest_nonfinal_slot = Some(update.slot);
            }
        }

        trace!("slot update done {}", update.slot);
        Ok(())
    }
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
        async_channel::unbounded::<AccountWrite>();

    // slot updates are not parallel because their order matters
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    let metric_con_retries = metrics_sender.register_u64("postgres_connection_retries".into());
    let metric_con_live = metrics_sender.register_u64("postgres_connections_alive".into());

    let postgres_slots =
        postgres_connection(&config, metric_con_retries.clone(), metric_con_live.clone()).await?;

    // postgres account write sending worker threads
    for _ in 0..config.account_write_connection_count {
        let postgres_account_writes =
            postgres_connection(&config, metric_con_retries.clone(), metric_con_live.clone())
                .await?;
        let account_write_queue_receiver_c = account_write_queue_receiver.clone();
        let account_tables_c = account_tables.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                // all of this needs to be in a function, to allow ?
                let write = account_write_queue_receiver_c
                    .recv()
                    .await
                    .expect("sender must stay alive");
                trace!(
                    "account write, channel size {}",
                    account_write_queue_receiver_c.len()
                );

                let mut error_count = 0;
                loop {
                    let client =
                        update_postgres_client(&mut client_opt, &postgres_account_writes, &config)
                            .await;
                    if let Err(err) = process_account_write(client, &write, &account_tables_c).await
                    {
                        error_count += 1;
                        if error_count - 1 < config.retry_query_max_count {
                            warn!("failed to process account write, retrying: {:?}", err);
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
            }
        });
    }

    // slot update handling thread
    let table_names: Vec<String> = account_tables
        .iter()
        .map(|table| table.table_name().to_string())
        .collect();
    let config = config.clone();
    tokio::spawn(async move {
        let mut slots_processing = SlotsProcessing {
            slots: HashMap::<i64, SlotUpdate>::new(),
            newest_nonfinal_slot: None,
            newest_final_slot: None,
            cleanup_table_sql: Vec::<String>::new(),
            metric_update_rooted: metrics_sender.register_u64("postgres_slot_update_rooted".into()),
            metric_update_uncles: metrics_sender.register_u64("postgres_slot_update_uncles".into()),
        };
        let mut client_opt = None;
        let mut metric_retries = metrics_sender.register_u64("postgres_slot_update_retries".into());

        slots_processing.set_cleanup_tables(&table_names);

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

            let mut error_count = 0;
            loop {
                let client =
                    update_postgres_client(&mut client_opt, &postgres_slots, &config).await;
                if let Err(err) = slots_processing.process(client, &update).await {
                    error_count += 1;
                    if error_count - 1 < config.retry_query_max_count {
                        warn!("failed to process slot update, retrying: {:?}", err);
                        metric_retries.increment();
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
        }
    });

    Ok((account_write_queue_sender, slot_queue_sender))
}
