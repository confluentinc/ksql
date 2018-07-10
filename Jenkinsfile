#!/usr/bin/env groovy

dockerfile {
    dockerUpstreamTag = '4.1.x-latest'  // Make sure PR builder points at a valid upstream tag.
    slackChannel = '#ksql-eng'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli', 'confluentinc/ksql-clickstream-demo', 'confluentinc/cp-ksql-cli', 'confluentinc/cp-ksql-server']
    extraBuildArgs = '-Ddocker.skip=false'
    extraDeployArgs = '-Ddocker.skip=true'
}
