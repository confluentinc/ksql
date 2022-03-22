#!/usr/bin/env groovy

common {
    slackChannel = '#ksqldb-quality-oncall'
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
    nanoVersion = true
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true"
}

