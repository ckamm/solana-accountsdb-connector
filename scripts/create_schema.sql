/**
 * This plugin implementation for PostgreSQL requires the following tables
 */

-- The table storing account writes, keeping only the newest write_version per slot
CREATE TABLE account_write (
    pubkey BYTEA NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    owner BYTEA,
    lamports BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    PRIMARY KEY (pubkey, slot, write_version)
);

-- The table storing slot information
CREATE TABLE slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status varchar(16) NOT NULL,
    uncle BOOL NOT NULL
);

