#!/usr/bin/env bash

set -e

if [[ -z $NEXUS_USERNAME || -z $NEXUS_PASSWORD ]]; then
  echo "Please export env NEXUS_USERNAME and NEXUS_PASSWORD with Nexus username and password"
  exit 1
fi

PIP_CONF=~/.pip/pip.conf

mkdir -p `dirname $PIP_CONF`

if [ -e $PIP_CONF ]; then
  echo "$PIP_CONF already exists and will be used"
else
  echo -e "[global]\nindex-url = https://$NEXUS_USERNAME:$NEXUS_PASSWORD@nexus.confluent.io/repository/pypi/simple" > $PIP_CONF
  echo "Created $PIP_CONF with Nexus repository"
fi
