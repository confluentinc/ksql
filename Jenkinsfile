#!/usr/bin/env groovy


parallel unitTests: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins -pl '!ksqldb-docker' -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -Dspotbugs.skip -Dmaven.site.skip"
    }
}, integrationTests: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins -pl '!ksqldb-docker' -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipTests"
    }
}
failFast:true
