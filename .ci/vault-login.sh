#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

# Login to Vault using the tools role ID and Secret ID.
#

export VAULT_ADDR=https://vault.cireops.gcp.internal.confluent.cloud

if [[ -z "$VAULT_ROLE_ID" ]]; then
  echo "Error: The VAULT_ROLE_ID is not set."
  exit 1
fi

if [[ -z "$VAULT_SECRET_ID" ]]; then
  echo "Error: The VAULT_SECRET_ID is not set."
  exit 1
fi

echo "Getting Vault token with tools Role ID and Secret ID"
VAULT_TOKEN=$(vault write -field=token auth/app/prod/login role_id="$VAULT_ROLE_ID" secret_id="$VAULT_SECRET_ID")

if [[ -z "$VAULT_TOKEN" ]]; then
  echo "Failed to get Vault token."
  exit 1
fi

echo "Logging into Vault with token."
if vault login "$VAULT_TOKEN" >/dev/null 2>&1; then
  echo "Successfully logged into Vault."
else
  echo "Failed to login to Vault."
  exit 1
fi
