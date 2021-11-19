#!/bin/bash

echo "$CONFIG_FILE" > config.toml
echo "$TLS_CA" > ca.pem
echo "$TLS_CLIENT" > client.pem

while true
do
	target/release/solana-accountsdb-connector-mango config.toml
	sleep 5
done
