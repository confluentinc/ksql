#!/usr/bin/env bash

set -e

if [[ -z $NEXUS_USERNAME || -z $NEXUS_PASSWORD ]]; then
  echo "Please export env NEXUS_USERNAME and NEXUS_PASSWORD with Nexus username and password"
  exit 1
fi

PYPIRC=~/.pypirc

if [ -e $PYPIRC ]; then
  echo "$PYPIRC already exists and will be used."
else
  echo "\
[distutils]
index-servers =
    nexus

[nexus]
repository: https://nexus.confluent.io/repository/pypi-internal/
username: $NEXUS_USERNAME
password: $NEXUS_PASSWORD
" > $PYPIRC
  echo "Created $PYPIRC with Nexus repository"
fi
