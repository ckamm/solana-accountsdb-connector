-- Views for raw accounts
CREATE VIEW account_rooted AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE slot.status = 'Rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
CREATE VIEW account_confirmed AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE (slot.status = 'Confirmed' AND NOT slot.uncle) OR slot.status = 'Rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
CREATE VIEW account_processed AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE ((slot.status = 'Confirmed' OR slot.status = 'Processed') AND NOT slot.uncle) OR slot.status = 'Rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
