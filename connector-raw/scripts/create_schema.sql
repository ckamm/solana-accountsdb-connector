/**
 * This plugin implementation for PostgreSQL requires the following tables
 */

CREATE TYPE "SlotStatus" AS ENUM (
    'Rooted',
    'Confirmed',
    'Processed'
);

CREATE TABLE monitoring (
    name TEXT PRIMARY KEY,
    last_update TIMESTAMP WITH TIME ZONE,
    last_slot_write TIMESTAMP WITH TIME ZONE,
    last_account_write_write TIMESTAMP WITH TIME ZONE,
    slot_queue BIGINT,
    account_write_queue BIGINT
);

CREATE TABLE pubkey (
    pubkey_id BIGSERIAL PRIMARY KEY,
    pubkey VARCHAR(44) NOT NULL UNIQUE
);

-- The table storing account writes, keeping only the newest write_version per slot
CREATE TABLE account_write (
    pubkey VARCHAR NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    owner VARCHAR NOT NULL,
    is_selected BOOL NOT NULL,
    lamports BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    PRIMARY KEY (pubkey, slot)
);
CREATE INDEX account_write_searchkey on account_write(pubkey, slot DESC);
CREATE INDEX account_write_pubkey_id_idx on account_write(pubkey);

-- The table storing slot information
CREATE TABLE slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status "SlotStatus" NOT NULL,
    uncle BOOL NOT NULL
);
CREATE INDEX ON slot (parent);