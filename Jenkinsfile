#!/usr/bin/env groovy

dockerfile {
    slackChannel = '#ksql-alerts'
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
}
