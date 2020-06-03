#!/usr/bin/env bash
#
# Copyright [2017 - 2020] Confluent Inc.
#

set -e

if [[ -z "${ARTIFACTORY_USERNAME}" || -z "${ARTIFACTORY_PASSWORD}" ]]; then
  echo "Please export env ARTIFACTORY_USERNAME and ARTIFACTORY_PASSWORD with Jfrog Artifactory username and password"
  exit 1
fi

PIP_CONF=~/.pip/pip.conf

mkdir -p `dirname ${PIP_CONF}`

if [ -e "${PIP_CONF}" ]; then
  echo "${PIP_CONF} already exists and will be used"
else
  echo -e "[global]\nindex-url = https://${ARTIFACTORY_USERNAME}:${ARTIFACTORY_PASSWORD}@confluent.jfrog.io/confluent/api/pypi/pypi/simple" > ${PIP_CONF}
  echo "Created ${PIP_CONF} with Jfrog Artifactory repository"
fi
