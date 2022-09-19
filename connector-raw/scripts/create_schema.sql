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

-- Returns a pubkey_id for a pubkey, by getting it from the table or inserting it.
-- Getting this fully correct is complex, see:
-- https://stackoverflow.com/questions/15939902/is-select-or-insert-in-a-function-prone-to-race-conditions/15950324
-- and currently this function assumes there are no deletions in the pubkey table!
CREATE OR REPLACE FUNCTION map_pubkey(_pubkey varchar(44), OUT _pubkey_id bigint)
  LANGUAGE plpgsql AS
$func$
BEGIN
   LOOP
      SELECT pubkey_id
      FROM   pubkey
      WHERE  pubkey = _pubkey
      INTO   _pubkey_id;

      EXIT WHEN FOUND;

      INSERT INTO pubkey AS t
      (pubkey) VALUES (_pubkey)
      ON     CONFLICT (pubkey) DO NOTHING
      RETURNING t.pubkey_id
      INTO   _pubkey_id;

      EXIT WHEN FOUND;
   END LOOP;
END
$func$;

-- The table storing account writes, keeping only the newest write_version per slot
CREATE TABLE account_write (
    pubkey VARCHAR NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    is_selected BOOL NOT NULL,
    lamports BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    PRIMARY KEY (pubkey, slot)
);
CREATE INDEX account_write_searchkey on account_write(pubkey, slot DESC, write_version DESC);
CREATE INDEX account_write_pubkey_id_idx on account_write(pubkey);

-- The table storing slot information
CREATE TABLE slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status "SlotStatus" NOT NULL,
    uncle BOOL NOT NULL
);
CREATE INDEX ON slot (parent);