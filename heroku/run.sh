#!/bin/bash

# Write the contents of env variable named by $1
printf '%s\n' "${!1}" > config.toml

echo "$TLS_CA" > ca.pem
echo "$TLS_CLIENT" > client.pem

while true
do
	target/release/solana-accountsdb-connector-mango config.toml
	sleep 5
done
