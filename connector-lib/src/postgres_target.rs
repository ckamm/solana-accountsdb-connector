use anyhow::Context;
use log::*;
use postgres_query::{query, FromSqlRow};
use std::{collections::HashMap, time::Duration};

use crate::{AccountWrite, SlotUpdate};

async fn postgres_connection(
    connection_string: &str,
) -> Result<async_channel::Receiver<Option<tokio_postgres::Client>>, anyhow::Error> {
    let (tx, rx) = async_channel::unbounded();

    let connection_string_c = connection_string.to_string();
    let mut initial =
        Some(tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await?);
    tokio::spawn(async move {
        loop {
            let (client, connection) = match initial.take() {
                Some(v) => v,
                None => {
                    let result =
                        tokio_postgres::connect(&connection_string_c, tokio_postgres::NoTls).await;
                    match result {
                        Ok(v) => v,
                        Err(err) => {
                            warn!("could not connect to postgres: {:?}", err);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }
            };

            tx.send(Some(client)).await.expect("send success");
            let result = connection.await;
            tx.send(None).await.expect("send success");
            warn!("postgres connection error: {:?}", result);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    Ok(rx)
}

async fn update_postgres_client<'a>(
    client: &'a mut Option<postgres_query::Caching<tokio_postgres::Client>>,
    rx: &async_channel::Receiver<Option<tokio_postgres::Client>>,
) -> &'a postgres_query::Caching<tokio_postgres::Client> {
    // get the most recent client, waiting if there's a disconnect
    while !rx.is_empty() || client.is_none() {
        // TODO: timeout configurable
        tokio::select! {
            client_raw_opt = rx.recv() => {
                *client = client_raw_opt.expect("not closed").map(|client| postgres_query::Caching::new(client));
            },
            _ = tokio::time::sleep(Duration::from_secs(360)) => {
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
) -> Result<(), anyhow::Error> {
    let pubkey: &[u8] = &write.pubkey.to_bytes();
    let owner: &[u8] = &write.owner.to_bytes();

    let query = query!(
        " \
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
    let _ = query.execute(client).await?;
    Ok(())
}

#[derive(Default)]
struct SlotsProcessing {
    slots: HashMap<i64, SlotUpdate>,
    newest_nonfinal_slot: Option<i64>,
    newest_final_slot: Option<i64>,
}

impl SlotsProcessing {
    async fn process(
        &mut self,
        client: &postgres_query::Caching<tokio_postgres::Client>,
        update: &SlotUpdate,
    ) -> Result<(), anyhow::Error> {
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
            let _ = query.execute(client).await.context("updating slot row")?;
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
            let _ = query.execute(client).await.context("updating slot row")?;
        }

        if update.status == "rooted" {
            self.slots.remove(&update.slot);

            // TODO: should also convert all parents to rooted, just in case we missed an update?
            // TODO: Or, instead, wipe some old slots, to avoid accumulating them needlessly?

            // Keep only the most recent final write per pubkey
            if self.newest_final_slot.unwrap_or(-1) < update.slot {
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
                let _ = query
                    .execute(client)
                    .await
                    .context("deleting old account writes")?;

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
                // update the uncle column for the chain of slots from the
                // newest down the the first rooted slot
                let query = query!("\
                    WITH RECURSIVE
                        liveslots AS (
                            SELECT slot.*, 0 AS depth FROM slot
                                WHERE slot = (SELECT max(slot) FROM slot)
                            UNION ALL
                            SELECT s.*, depth + 1 FROM slot s
                                INNER JOIN liveslots l ON s.slot = l.parent
                                WHERE l.status != 'rooted' AND depth < 1000
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

        info!("slot update done {}", update.slot);
        Ok(())
    }
}

pub async fn init(
    connection_string: &str,
) -> Result<
    (
        async_channel::Sender<AccountWrite>,
        async_channel::Sender<SlotUpdate>,
    ),
    anyhow::Error,
> {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // slot updates are not parallel because their order matters
    let (slot_queue_sender, slot_queue_receiver) = async_channel::unbounded::<SlotUpdate>();

    let postgres_slots = postgres_connection(connection_string).await?;

    // postgres account write sending worker threads
    // TODO: thread count config
    for _ in 0..4 {
        let postgres_account_writes = postgres_connection(connection_string).await?;
        let account_write_queue_receiver_c = account_write_queue_receiver.clone();
        tokio::spawn(async move {
            let mut client_opt = None;
            loop {
                // all of this needs to be in a function, to allow ?
                let write = account_write_queue_receiver_c
                    .recv()
                    .await
                    .expect("sender must stay alive");
                info!(
                    "account write, channel size {}",
                    account_write_queue_receiver_c.len()
                );

                let mut error_count = 0;
                loop {
                    let client =
                        update_postgres_client(&mut client_opt, &postgres_account_writes).await;
                    if let Err(err) = process_account_write(client, &write).await {
                        error_count += 1;
                        if error_count - 1 < 3 {
                            warn!("failed to process account write, retrying: {:?}", err);
                            tokio::time::sleep(Duration::from_secs(1)).await;
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
    tokio::spawn(async move {
        let mut slots_processing = SlotsProcessing::default();
        let mut client_opt = None;

        loop {
            let update = slot_queue_receiver
                .recv()
                .await
                .expect("sender must stay alive");
            info!(
                "slot update {}, channel size {}",
                update.slot,
                slot_queue_receiver.len()
            );

            let mut error_count = 0;
            loop {
                let client = update_postgres_client(&mut client_opt, &postgres_slots).await;
                if let Err(err) = slots_processing.process(client, &update).await {
                    error_count += 1;
                    if error_count - 1 < 3 {
                        warn!("failed to process slot update, retrying: {:?}", err);
                        tokio::time::sleep(Duration::from_secs(1)).await;
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
