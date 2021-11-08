-- Views for raw accounts
CREATE VIEW account_rooted AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE slot.status = 'rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
CREATE VIEW account_committed AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE (slot.status = 'committed' AND NOT slot.uncle) OR slot.status = 'rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
CREATE VIEW account_processed AS
    SELECT
        DISTINCT ON(pubkey)
        *
        FROM account_write
        INNER JOIN slot USING(slot)
        WHERE ((slot.status = 'committed' OR slot.status = 'processed') AND NOT slot.uncle) OR slot.status = 'rooted'
        ORDER BY pubkey, slot DESC, write_version DESC;
