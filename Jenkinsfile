#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksql-alerts'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-cli']
    extraDeployArgs = '-Ddocker.skip=true'
}
