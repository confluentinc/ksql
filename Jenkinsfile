#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksql-alerts' : '#ksqldb-warn'

common {
    slackChannel = channel
    upstreamProjects = 'confluentinc/schema-registry'
    disableConcurrentBuilds = true
}

