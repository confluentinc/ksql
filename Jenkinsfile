#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksql-alerts' : '#ksqldb-warn'

dockerfile {
    slackChannel = channel
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli', 'confluentinc/cp-ksql-cli', 'confluentinc/cp-ksql-server']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
}

