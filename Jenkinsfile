#!/usr/bin/env groovy
common {
    slackChannel = '#ksqldb-quality-oncall'
    nodeLabel = 'docker-debian-jdk8'
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
    nanoVersion = true
    timeoutHours = 3
    upstreamProjects = 'confluentinc/schema-registry'
    mavenProfiles = 'jenkins -Dmaven.gitcommitid.nativegit=true -Ddocker.skip=true -Pprofile -DprofileFormat=CONSOLE'
}

