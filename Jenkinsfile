#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksqldb-quality-oncall' : '#ksqldb-warn'

// removed confluent-security-plugins and confluent-cloud-plugins from downstream due to Jenkins deprecation
def downStreams = "${env.BRANCH_NAME}".contains('master') ? 
    ["cc-docker-ksql"] :
    []

common {
    nodeLabel = 'docker-debian-jdk11'
    slackChannel = channel
    timeoutHours = 5
    upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    downStreamRepos = downStreams
    downStreamValidate = false
    nanoVersion = true
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true"
    mavenBuildGoals = "clean install"
    runMergeCheck = false
    mvnSkipDeploy = true
}

