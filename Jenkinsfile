#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksqldb-quality-oncall' : '#ksqldb-warn'

common {
    nodeLabel = 'docker-openjdk11'
    slackChannel = channel
    timeoutHours = 4
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    disableConcurrentBuilds = true
    downStreamValidate = false
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true"
}

