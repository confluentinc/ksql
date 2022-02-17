#!/usr/bin/env groovy

def channel = env.BRANCH_NAME.equals('master') ? '#ksql-alerts' : '#ksqldb-warn'

common {
    slackChannel = channel
    upstreamProjects = 'confluentinc/schema-registry'
    disableConcurrentBuilds = true
}

