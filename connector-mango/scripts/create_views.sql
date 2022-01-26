-- Views for raw accounts
CREATE VIEW account_rooted AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW account_confirmed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND ((slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW account_processed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;

CREATE VIEW mango_account_rooted AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_account_write.*
        FROM mango_account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_account_confirmed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_account_write.*
        FROM mango_account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND ((slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_account_processed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_account_write.*
        FROM mango_account_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;

CREATE VIEW mango_account_processed_balance AS
    SELECT
        pubkey,
        unnest(array['MNGO', 'BTC', 'ETH', 'SOL', 'USDT', 'SRM', 'RAY', 'COPE', 'FTT', 'ADA', 'unused10', 'unused11', 'unused12', 'unused13', 'unused14', 'USDC']) as token,
        unnest(deposits) as deposit,
        unnest(borrows) as borrow
    FROM mango_account_processed;

CREATE VIEW mango_account_processed_perp AS
    SELECT
        pubkey,
        perp,
        (q.perp_account).*
    FROM (
        SELECT
            pubkey,
            unnest(array['MNGO', 'BTC', 'ETH', 'SOL', 'unused_USDT', 'SRM', 'RAY', 'unused_COPE', 'FTT', 'ADA', 'unused10', 'unused11', 'unused12', 'unused13', 'unused14']) as perp,
            unnest(perp_accounts) as perp_account
        FROM mango_account_processed
    ) q;

CREATE VIEW mango_group_rooted AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_group_write.*
        FROM mango_group_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_group_confirmed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_group_write.*
        FROM mango_group_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND ((slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_group_processed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_group_write.*
        FROM mango_group_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;

CREATE VIEW mango_cache_rooted AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_cache_write.*
        FROM mango_cache_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_cache_confirmed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_cache_write.*
        FROM mango_cache_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND ((slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
CREATE VIEW mango_cache_processed AS
    SELECT pubkey, latest_writes.* FROM
        (SELECT
        DISTINCT ON(pubkey_id)
        mango_cache_write.*
        FROM mango_cache_write
        LEFT JOIN slot USING(slot)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC) latest_writes
    LEFT JOIN account_write USING(pubkey_id, slot, write_version)
    LEFT JOIN pubkey USING(pubkey_id)
    WHERE is_selected;
