#!/usr/bin/env groovy

common {
    slackChannel = '#ksqldb-quality-oncall'
    timeoutHours = 6
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins", "cc-docker-ksql"]
    downStreamValidate = false
    nanoVersion = true
    pinnedNanoVersions = false
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true"
    nodeLabel = 'docker-debian-10-jdk8'
}

