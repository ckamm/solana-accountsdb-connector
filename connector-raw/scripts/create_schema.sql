/**
 * This plugin implementation for PostgreSQL requires the following tables
 */

CREATE TYPE "SlotStatus" AS ENUM (
    'Rooted',
    'Confirmed',
    'Processed'
);

-- The table storing account writes, keeping only the newest write_version per slot
CREATE TABLE account_write (
    pubkey VARCHAR(44) NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    owner VARCHAR(44),
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
    status "SlotStatus" NOT NULL,
    uncle BOOL NOT NULL
);
CREATE INDEX ON slot (parent);