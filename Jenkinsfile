#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksqldb-quality-oncall' : '#ksqldb-warn'

def downStreams = "${env.BRANCH_NAME}".contains('master') ? 
    ["confluent-security-plugins", "confluent-cloud-plugins", "cc-docker-ksql"] :
    ["confluent-security-plugins", "confluent-cloud-plugins"]
