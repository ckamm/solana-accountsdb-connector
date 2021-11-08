Overview
========

This project is about streaming Solana account updates for a specific program
into other databases or event queues.

Supported Solana sources:
- AccountsDB plugin (preferred) plus JSONRPC HTTP API (for initial snapshots)

Unfinished Solana sources:
- JSONRPC websocket subscriptions plus JSONRPC HTTP API (for initial snapshots)

Supported targets:
- PostgreSQL


Components
==========

- `accountsdb-plugin-grpc/`

  The Solana AccountsDB plugin. It opens a gRPC server (see `proto/`) and
  broadcasts account and slot updates to all clients that connect.

- `lib/`

  The connector abstractions that the connector service is built from.

  Projects may want to use it to build their own connector service and decode
  their specific account data before sending it into target systems.

- `connector-raw/`

  A connector binary built on lib/ that stores raw binary account data in
  PostgreSQL.

- `connector-mango/`

  A connector binary built on lib/ that decodes Mango account types before
  storing them in PostgeSQL.


Design and Reliability
======================

```
Solana    --------------->   Connector   ----------->   PostgreSQL
 node       jsonrpc/gRPC      service
```

For reliability it is recommended to feed data from multiple Solana nodes into
the Connector service. (That's not yet fully supported)

It is also allowed to run multiple Connector services that target the same
PostgeSQL target database. (That's not yet fully supported)

The Connector service is stateless (except for some caches). Restarting it is
always safe.

If the Solana node is down, the Connector service attempts to reconnect and
then requests a new data snapshot if necessary.

If PostgeSQL is down temporarily, the Connector service caches updates and
applies them when the database is back up.

If PostgreSQL is down for a longer time, the Connector service exits with
an error. On restart, it pauses until PostgreSQL is back up, and then starts
pulling data from the Solana nodes again.


PostgreSQL data layout
======================

See `scripts/` for SQL that creates the target schema.

The Connector streams data into the `account_write` and `slot` tables. When
slots become "rooted", older `account_write` data rooted slots is deleted. That
way the current account data for the latest rooted, committed or processed slot
can be queried, but older data is forgotten.

When new slots arrive, the `uncle` column is updated for "processed" and
"committed" slots to allow easy filtering of slots that are no longer part of
the chain.

Example for querying committed data:
```
SELECT DISTINCT ON(pubkey) *
FROM account_write
INNER JOIN slot USING(slot)
WHERE status = "rooted" OR (uncle = FALSE AND status = "committed")
ORDER BY pubkey, slot DESC, write_version DESC;
```

For each pubkey, this gets the latest (most recent slot, most recent
write_version) account data; limited to slots that are either rooted or
(committed and not an uncle).
