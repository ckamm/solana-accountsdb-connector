/**
 * Script for cleaning up the schema for PostgreSQL used for the AccountsDb plugin.
 */

DROP TABLE slot CASCADE;
DROP TABLE account_write CASCADE;
DROP TABLE mango_group_write CASCADE;
DROP TABLE mango_cache_write CASCADE;
DROP TABLE mango_account_write CASCADE;

DROP TYPE "PerpAccount";
DROP TYPE "TokenInfo";
DROP TYPE "SpotMarketInfo";
DROP TYPE "PerpMarketInfo";
DROP TYPE "PriceCache";
DROP TYPE "RootBankCache";
DROP TYPE "PerpMarketCache";
