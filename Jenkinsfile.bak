#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksql-eng'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli', 'confluentinc/ksql-clickstream-demo', 'confluentinc/cp-ksql-server', 'confluentinc/cp-ksql-cli']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
}
