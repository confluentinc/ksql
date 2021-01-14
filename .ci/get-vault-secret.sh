#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

# Get a secret from Vault. You must already be logged in using the vault-login.sh script.
# You must provide the path to the secret and the secret key for which you want the value.
# The path to the secret is relative to v1/ci/kv
#
# Arguments:
# $1 The path to the secret relative to v1/ci/kv
# $2 The key in the secret you want the value for
#

set +x

SECRET=$1
KEY=$2
export VAULT_ADDR=https://vault.cireops.gcp.internal.confluent.cloud

if SECRET_VALUE=$(vault kv get -field "$KEY" v1/ci/kv/"$SECRET"); then
  echo "$SECRET_VALUE"
else
  echo "Failed to get secret and key: $SECRET, $KEY"
  exit 1
fi
