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
