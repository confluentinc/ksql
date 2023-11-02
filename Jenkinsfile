#!/usr/bin/env groovy

def channel = "${env.BRANCH_NAME}".contains('master') ? '#ksqldb-quality-oncall' : '#ksqldb-warn'

def downStreams = "${env.BRANCH_NAME}".contains('master') ? 
    ["confluent-security-plugins", "confluent-cloud-plugins", "cc-docker-ksql"] :
    ["confluent-security-plugins", "confluent-cloud-plugins"]

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
    pinnedNanoVersions = true
    maxBuildsToKeep = 99
    maxDaysToKeep = 90
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -DskipTests -DskipIntegrationTests"
    mavenBuildGoals = "clean install"
    runMergeCheck = false
    mvnSkipDeploy = true
}

