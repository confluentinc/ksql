#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksqldb-alerts'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli', 'confluentinc/cp-ksql-cli', 'confluentinc/cp-ksql-server']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
}
