#!/usr/bin/env groovy

parallel unittests: {

dockerfile {
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
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16"
}
}, integrationtests: {

dockerfile {
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
    extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16"
}
}
failFast: true
