#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksql-alerts' : '#ksqldb-warn'

dockerfile {
    slackChannel = channel
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-cli']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
    disableConcurrentBuilds = true
}

