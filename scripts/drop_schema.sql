/**
 * Script for cleaning up the schema for PostgreSQL used for the AccountsDb plugin.
 */

DROP TABLE account_write CASCADE;
DROP TABLE mango_account_write CASCADE;
DROP TABLE slot CASCADE;

DROP TYPE "PerpAccount";
