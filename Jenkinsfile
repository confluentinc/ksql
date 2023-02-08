#!/usr/bin/env groovy

common {
    slackChannel = '#ksqldb-quality-oncall'
    timeoutHours = 5
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
    downStreamValidate = false
    nanoVersion = true
    pinnedNanoVersions = true
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true"
    mavenBuildGoals = "clean install"
}

