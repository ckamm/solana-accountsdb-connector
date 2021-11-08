-- Views for raw accounts
CREATE VIEW account_rooted AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        INNER JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        WHERE slot.status = 'Rooted'
        ORDER BY pubkey_id, slot DESC, write_version DESC;
CREATE VIEW account_confirmed AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        INNER JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        WHERE (slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted'
        ORDER BY pubkey_id, slot DESC, write_version DESC;
CREATE VIEW account_processed AS
    SELECT
        DISTINCT ON(pubkey_id)
        pubkey, account_write.*
        FROM account_write
        INNER JOIN slot USING(slot)
        INNER JOIN pubkey USING(pubkey_id)
        WHERE ((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted'
        ORDER BY pubkey_id, slot DESC, write_version DESC;
