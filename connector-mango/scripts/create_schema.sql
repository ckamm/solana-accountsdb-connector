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

CREATE OR REPLACE FUNCTION map_pubkey_arr(_pubkey_arr varchar(44)[], OUT _pubkey_id_arr bigint[])
  LANGUAGE plpgsql AS
$func$
BEGIN
   FOR i IN array_lower(_pubkey_arr, 1)..array_upper(_pubkey_arr, 1) LOOP
      _pubkey_id_arr[i] := map_pubkey(_pubkey_arr[i]);
   END LOOP;
END
$func$
RETURNS NULL ON NULL INPUT;

-- The table storing account writes, keeping only the newest write_version per slot
CREATE TABLE account_write (
    pubkey_id BIGINT NOT NULL REFERENCES pubkey,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    owner_id BIGINT REFERENCES pubkey,
    lamports BIGINT NOT NULL,
    executable BOOL NOT NULL,
    rent_epoch BIGINT NOT NULL,
    data BYTEA,
    PRIMARY KEY (pubkey_id, slot, write_version)
);
CREATE UNIQUE INDEX account_write_searchkey on account_write(pubkey_id, slot DESC, write_version DESC);

-- The table storing slot information
CREATE TABLE slot (
    slot BIGINT PRIMARY KEY,
    parent BIGINT,
    status "SlotStatus" NOT NULL,
    uncle BOOL NOT NULL
);
CREATE INDEX ON slot (parent);

CREATE TYPE "PerpAccount" AS (
    base_position INT8,
    quote_position NUMERIC, -- I80F48
    long_settled_funding NUMERIC, -- I80F48
    short_settled_funding NUMERIC, -- I80F48
    bids_quantity INT8,
    asks_quantity INT8,
    taker_base INT8,
    taker_quote INT8,
    mngo_accrued NUMERIC -- u64
);

CREATE TABLE mango_account_write (
    pubkey_id BIGINT NOT NULL REFERENCES pubkey,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    mango_group_id BIGINT REFERENCES pubkey,
    owner_id BIGINT REFERENCES pubkey,
    in_margin_basket BOOL[],
    num_in_margin_basket INT2,
    deposits NUMERIC[], -- I80F48[]
    borrows NUMERIC[], -- I80F48[]
    spot_open_orders_ids BIGINT[],
    perp_accounts "PerpAccount"[],
    order_market INT2[],
    order_side INT2[],
    orders NUMERIC[], -- i128[]
    client_order_ids NUMERIC[], -- u64[]
    msrm_amount NUMERIC, -- u64
    being_liquidated BOOL,
    is_bankrupt BOOL,
    info BYTEA,
    advanced_orders_key_id BIGINT REFERENCES pubkey,
    padding BYTEA,
    PRIMARY KEY (pubkey_id, slot, write_version)
);
CREATE UNIQUE INDEX mango_account_write_searchkey on mango_account_write(pubkey_id, slot DESC, write_version DESC);


CREATE TYPE "TokenInfo" AS (
    mint varchar(44), -- TODO: also use pubkey table? but is unergonomic
    root_bank varchar(44),
    decimals INT2,
    padding BYTEA
);

CREATE TYPE "SpotMarketInfo" AS (
    spot_market varchar(44),
    maint_asset_weight NUMERIC, -- all I80F48
    init_asset_weight NUMERIC,
    maint_liab_weight NUMERIC,
    init_liab_weight NUMERIC,
    liquidation_fee NUMERIC
);

CREATE TYPE "PerpMarketInfo" AS (
    perp_market varchar(44),
    maint_asset_weight NUMERIC, -- all I80F48
    init_asset_weight NUMERIC,
    maint_liab_weight NUMERIC,
    init_liab_weight NUMERIC,
    liquidation_fee NUMERIC,
    maker_fee NUMERIC,
    taker_fee NUMERIC,
    base_lot_size INT8,
    quote_lot_size INT8
);

CREATE TABLE mango_group_write (
    pubkey_id BIGINT NOT NULL REFERENCES pubkey,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    num_oracles INT8, -- technically usize, but it's fine
    tokens "TokenInfo"[],
    spot_markets "SpotMarketInfo"[],
    perp_markets "PerpMarketInfo"[],
    oracle_ids BIGINT[],
    signer_nonce NUMERIC, -- u64
    signer_key_id BIGINT REFERENCES pubkey,
    admin_id BIGINT REFERENCES pubkey,
    dex_program_id BIGINT REFERENCES pubkey,
    mango_cache_id BIGINT REFERENCES pubkey,
    valid_interval NUMERIC, -- u64
    insurance_vault_id BIGINT REFERENCES pubkey,
    srm_vault_id BIGINT REFERENCES pubkey,
    msrm_vault_id BIGINT REFERENCES pubkey,
    fees_vault_id BIGINT REFERENCES pubkey,
    padding BYTEA,
    PRIMARY KEY (pubkey_id, slot, write_version)
);
CREATE UNIQUE INDEX mango_group_write_searchkey on mango_group_write(pubkey_id, slot DESC, write_version DESC);

CREATE TYPE "PriceCache" AS (
    price NUMERIC, -- I80F48
    last_update NUMERIC -- u64
);

CREATE TYPE "RootBankCache" AS (
    deposit_index NUMERIC, -- I80F48
    borrow_index NUMERIC, -- I80F48
    last_update NUMERIC -- u64
);

CREATE TYPE "PerpMarketCache" AS (
    long_funding NUMERIC, -- I80F48
    short_funding NUMERIC, -- I80F48
    last_update NUMERIC -- u64
);

CREATE TABLE mango_cache_write (
    pubkey_id BIGINT NOT NULL REFERENCES pubkey,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    price_cache "PriceCache"[],
    root_bank_cache "RootBankCache"[],
    perp_market_cache "PerpMarketCache"[],
    PRIMARY KEY (pubkey_id, slot, write_version)
);
CREATE UNIQUE INDEX mango_cache_write_searchkey on mango_cache_write(pubkey_id, slot DESC, write_version DESC);
