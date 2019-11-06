#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksql-alerts'
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRepos = ['confluentinc/ksql-cli']
    extraBuildArgs = '-Dskip.docker.build=false'
    extraDeployArgs = '-Dskip.docker.build=true'
}
