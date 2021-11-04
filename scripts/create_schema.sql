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

CREATE TYPE "PerpAccount" AS (
    base_position INT8,
    quote_position DOUBLE PRECISION, -- I80F48
    long_settled_funding DOUBLE PRECISION, -- I80F48
    short_settled_funding DOUBLE PRECISION, -- I80F48
    bids_quantity INT8,
    asks_quantity INT8,
    taker_base INT8,
    taker_quote INT8,
    mngo_accrued INT8 -- u64
);

CREATE TABLE mango_account_write (
    pubkey BYTEA NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    mango_group BYTEA,
    owner BYTEA,
    in_margin_basket BOOL[],
    num_in_margin_basket INT2,
    --deposits NUMERIC(41,16)[],
    --borrows NUMERIC(41,16)[],
    deposits DOUBLE PRECISION[],
    borrows DOUBLE PRECISION[],
    spot_open_orders BYTEA[],
    perp_accounts "PerpAccount"[],
    order_market INT2[],
    order_side INT2[],
    --orders NUMERIC(39)[],
    orders DOUBLE PRECISION[],
    --client_order_ids NUMERIC(20)[],
    client_order_ids INT8[],
    --msrm_amount NUMERIC(20),
    msrm_amount INT8,
    being_liquidated BOOL,
    is_bankrupt BOOL,
    info BYTEA,
    advanced_orders_key BYTEA,
    padding BYTEA,
    PRIMARY KEY (pubkey, slot, write_version)
);
