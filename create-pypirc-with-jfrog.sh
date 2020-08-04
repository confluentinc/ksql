#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

set -e

if [[ -z $ARTIFACTORY_USERNAME || -z $ARTIFACTORY_PASSWORD ]]; then
  echo "Please export env ARTIFACTORY_USERNAME and ARTIFACTORY_PASSWORD with Jfrog Artifactory username and password"
  exit 1
fi

PYPIRC=~/.pypirc

if [ -e $PYPIRC ]; then
  echo "$PYPIRC already exists and will be used."
else
  echo "\
[distutils]
index-servers =
    jfrog

[jfrog]
repository: https://confluent.jfrog.io/confluent/api/pypi/pypi-internal
username: $ARTIFACTORY_USERNAME
password: $ARTIFACTORY_PASSWORD
" > $PYPIRC
  echo "Created $PYPIRC with Jfrog Artifactory repository"
fi
