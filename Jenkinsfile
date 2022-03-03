#!/usr/bin/env groovy

parallel allButUnittests: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = 'jenkins  -Ddocker.skip=true -Pprofile -DprofileFormat=CONSOLE -DskipITs -DskipTests'
    }
}, integrationTests: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipTests -Dspotbugs.skip -Dmaven.site.skip"
    }
}, unittestsExcludeRestAndFunctional: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -Dspotbugs.skip -Dmaven.site.skip '-Dtest=!%regex[.*.ksql.rest.*],!%regex[.*.ksql.test.*]' -DfailIfNoTests=false"
    }
}, unittestsIncludeFunctional: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins  -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.test.*]' -DfailIfNoTests=false"
    }
}, unittestsIncludeRest: {
    common {
        slackChannel = '#ksqldb-quality-oncall'
        nodeLabel = 'docker-debian-jdk8'
        downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
        nanoVersion = true
        timeoutHours = 3
        upstreamProjects = 'confluentinc/schema-registry'
        mavenProfiles = "jenkins  -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.rest.*]' -DfailIfNoTests=false"
    }
}
failFast: true

