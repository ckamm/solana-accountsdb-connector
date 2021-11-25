-- Views for raw accounts
CREATE VIEW account_rooted AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC;
CREATE VIEW account_confirmed AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND ((slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC;
CREATE VIEW account_processed AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        LEFT JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        CROSS JOIN (SELECT max(slot) FROM slot) ms
        WHERE slot <= ms.max
        AND (((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted' OR slot.status is NULL)
        ORDER BY pubkey_id, slot DESC, write_version DESC;
