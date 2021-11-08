/**
 * Script for cleaning up the schema for PostgreSQL used for the AccountsDb plugin.
 */

DROP TABLE slot CASCADE;
DROP TABLE account_write CASCADE;
DROP TABLE pubkey CASCADE;
DROP TYPE "SlotStatus";