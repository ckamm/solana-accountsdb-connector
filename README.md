Overview
========

This project is about streaming Solana account updates for a specific program
into other databases or event queues.

Having an up to date version of all account data data in a database is
particularly useful for queries that need access to all accounts. For example,
retrieving the addresses of Mango Markets accounts with the largest unrealized
PnL goes from "getProgramAccounts from a Solana node for 50MB of data and compute
locally (3-10s total)" to "run a SQL query (150ms total)".

The database could also be used as a backend for serving `getMultipleAccounts`
and `getProgramAccounts` queries generally. That would reduce load on Solana RPCt
nodes while decreasing response times.

Supported Solana sources:
- Geyser plugin (preferred) plus JSONRPC HTTP API (for initial snapshots)

Unfinished Solana sources:
- JSONRPC websocket subscriptions plus JSONRPC HTTP API (for initial snapshots)

Supported targets:
- PostgreSQL


Components
==========

- [`geyser-plugin-grpc/`](geyser-plugin-grpc/)

  The Solana Geyser plugin. It opens a gRPC server (see [`proto/`](proto/)) and
  broadcasts account and slot updates to all clients that connect.

- [`lib/`](lib/)

  The connector abstractions that the connector service is built from.

  Projects may want to use it to build their own connector service and decode
  their specific account data before sending it into target systems.

- [`connector-raw/`](connector-raw/)

  A connector binary built on lib/ that stores raw binary account data in
  PostgreSQL.

- [`connector-mango/`](connector-mango/)

  A connector binary built on lib/ that decodes Mango account types before
  storing them in PostgeSQL.


Setup Tutorial
==============

1. Compile the project.

   Make sure that you are using _exactly_ the same Rust version for compiling the
   Geyser plugin that was used for compiling your `solana-validator`! Otherwise
   the plugin will crash the validator during startup!

2. Prepare the plugin configuration file.

   [Here is an example](geyser-plugin-grpc/example-config.json). This file
   points the validator to your plugin shared library, controls which accounts
   will be exported, which address the gRPC server will bind to and internal
   queue sizes.

3. Run `solana-validator` with `--geyser-plugin-config myconfig.json`.

   Check the logs to ensure the plugin was loaded.

4. Prepare the connector configuration file.

   [Here is an example](connector-raw/example-config.toml).

   - `rpc_ws_url` is unused and can stay empty.
   - `connection_string` for your `grpc_sources` must point to the gRPC server
     address configured for the plugin.
   - `rpc_http_url` must point to the JSON-RPC URL.
   - `connection_string` for your `posgres_target` uses [the tokio-postgres syntax](https://docs.rs/tokio-postgres/0.7.5/tokio_postgres/config/struct.Config.html)
   - `program_id` must match what is configured for the gRPC plugin

5. Prepare the PostgreSQL schema.

   Use [this example script](connector-raw/scripts/create_schema.sql).

6. Start the connector service binary.

   Pass the path to the config file as the first argument. It logs to stdout.
   It should be restarted on exit. (it intentionally terminates when postgres is
   unreachable for too long, for example)

7. Monitor the logs

   `WARN` messages can be recovered from. `ERROR` messages need attention.

   Check the metrics for `account_write_queue` and `slot_update_queue`: They should
   be around 0. If they keep growing the service can't keep up and you'll need
   to figure out what's up.


Design and Reliability
======================

```
Solana    --------------->   Connector   ----------->   PostgreSQL
 nodes      jsonrpc/gRPC       nodes
```

For reliability it is recommended to feed data from multiple Solana nodes into
each Connector node.

It is also allowed to run multiple Connector nodes that target the same
PostgeSQL target database.

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
way the current account data for the latest rooted, confirmed or processed slot
can be queried, but older data is forgotten.

When new slots arrive, the `uncle` column is updated for "processed" and
"confirmed" slots to allow easy filtering of slots that are no longer part of
the chain.

Example for querying confirmed data:
```
SELECT DISTINCT ON(pubkey_id)
    pubkey, account_write.*
FROM account_write
LEFT JOIN slot USING(slot)
INNER JOIN pubkey USING(pubkey_id)
WHERE status = 'Rooted' OR status IS NULL OR (uncle = FALSE AND status = 'Confirmed')
ORDER BY pubkey_id, slot DESC, write_version DESC;
```

For each pubkey, this gets the latest (most recent slot, most recent
write_version) account data; limited to slots that are either rooted or
(confirmed and not an uncle).
