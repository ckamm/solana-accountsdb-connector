/**
 * This plugin implementation for PostgreSQL requires the following tables
 */

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
    status VARCHAR(16) NOT NULL,
    uncle BOOL NOT NULL
);

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
    pubkey VARCHAR(44) NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    mango_group VARCHAR(44),
    owner VARCHAR(44),
    in_margin_basket BOOL[],
    num_in_margin_basket INT2,
    deposits NUMERIC[], -- I80F48[]
    borrows NUMERIC[], -- I80F48[]
    spot_open_orders VARCHAR(44)[],
    perp_accounts "PerpAccount"[],
    order_market INT2[],
    order_side INT2[],
    orders NUMERIC[], -- i128[]
    client_order_ids NUMERIC[], -- u64[]
    msrm_amount NUMERIC, -- u64
    being_liquidated BOOL,
    is_bankrupt BOOL,
    info BYTEA,
    advanced_orders_key VARCHAR(44),
    padding BYTEA,
    PRIMARY KEY (pubkey, slot, write_version)
);


CREATE TYPE "TokenInfo" AS (
    mint VARCHAR(44),
    root_bank VARCHAR(44),
    decimals INT2,
    padding BYTEA
);

CREATE TYPE "SpotMarketInfo" AS (
    spot_market VARCHAR(44),
    maint_asset_weight NUMERIC, -- all I80F48
    init_asset_weight NUMERIC,
    maint_liab_weight NUMERIC,
    init_liab_weight NUMERIC,
    liquidation_fee NUMERIC
);

CREATE TYPE "PerpMarketInfo" AS (
    perp_market VARCHAR(44),
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
    pubkey VARCHAR(44) NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    num_oracles INT8, -- technically usize, but it's fine
    tokens "TokenInfo"[],
    spot_markets "SpotMarketInfo"[],
    perp_markets "PerpMarketInfo"[],
    oracles VARCHAR(44)[],
    signer_nonce NUMERIC, -- u64
    signer_key VARCHAR(44),
    "admin" VARCHAR(44),
    dex_program_id VARCHAR(44),
    mango_cache VARCHAR(44),
    valid_interval NUMERIC, -- u64
    insurance_vault VARCHAR(44),
    srm_vault VARCHAR(44),
    msrm_vault VARCHAR(44),
    fees_vault VARCHAR(44),
    padding BYTEA,
    PRIMARY KEY (pubkey, slot, write_version)
);

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
    pubkey VARCHAR(44) NOT NULL,
    slot BIGINT NOT NULL,
    write_version BIGINT NOT NULL,
    version INT2,
    is_initialized BOOL,
    extra_info BYTEA,
    price_cache "PriceCache"[],
    root_bank_cache "RootBankCache"[],
    perp_market_cache "PerpMarketCache"[],
    PRIMARY KEY (pubkey, slot, write_version)
);
