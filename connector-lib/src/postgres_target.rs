use log::*;
use postgres_query::{query, FromSqlRow};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, RwLock},
};

use crate::{AccountWrite, SlotUpdate};

pub fn init(
    connection_string: &str,
) -> (
    crossbeam_channel::Sender<AccountWrite>,
    crossbeam_channel::Sender<SlotUpdate>,
) {
    // The actual message may want to also contain a retry count, if it self-reinserts on failure?
    let (account_write_queue_sender, account_write_queue_receiver) =
        crossbeam_channel::unbounded::<AccountWrite>();

    // slot updates are not parallel because their order matters
    let (slot_queue_sender, slot_queue_receiver) = crossbeam_channel::unbounded::<SlotUpdate>();

    // the postgres connection management thread
    // - creates a connection and runs it
    // - if it fails, reestablishes it and requests a new snapshot
    let postgres_client: Arc<RwLock<Arc<Option<postgres_query::Caching<tokio_postgres::Client>>>>> =
        Arc::new(RwLock::new(Arc::new(None)));
    let postgres_client_c = Arc::clone(&postgres_client);
    let connection_string_c = connection_string.to_string();
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
    let connection_string_c = connection_string.to_string();
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
                info!("new write: count: {}", result);
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
            info!(
                "slot update {}, channel size {}",
                update.slot,
                slot_queue_receiver.len()
            );

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
                info!("new slot: count: {}", result);
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
                info!("new slot: count: {}", result);
            }

            if update.status == "rooted" {
                info!("slot changed to rooted");
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
                    info!("recomputing uncles");
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
                    let result = query.execute(client).await.unwrap();
                }

                if new_newest_slot {
                    newest_nonfinal_slot = Some(update.slot);
                }
            }

            info!("slot update done {}", update.slot);
        }
    });

    (account_write_queue_sender, slot_queue_sender)
}
