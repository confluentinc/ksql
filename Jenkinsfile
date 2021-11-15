#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksql-alerts' : '#ksqldb-warn'

dockerfile {
    slackChannel = channel
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    disableConcurrentBuilds = true
}

