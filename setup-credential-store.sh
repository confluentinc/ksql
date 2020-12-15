#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

set -e

if [[ -z $GIT_CREDENTIAL ]]; then
  echo "Please export GIT_CREDENTIAL to contain git username:password"
  exit 1
fi

git config --global credential.helper store
echo "https://$GIT_CREDENTIAL@github.com" > ~/.git-credentials
