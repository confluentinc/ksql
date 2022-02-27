#!/usr/bin/env groovy

parallel allButUnittests: {
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dmaven.artifact.threads=16 -DskipITs -DskipTests"
    }
}, unittestsExcludeRestAndFunctional: {
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -Dspotbugs.skip -Dmaven.site.skip '-Dtest=!%regex[.*.ksql.rest.*],!%regex[.*.ksql.test.*]' -DfailIfNoTests=false"
    }
}, unittestsIncludeRest: {
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.rest.*]' -DfailIfNoTests=false"
    }
}, unittestsIncludeFunctional: {
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.test.*]' -DfailIfNoTests=false"
    }
}, integrationTests: {
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipTests -Dspotbugs.skip -Dmaven.site.skip"
    }
}  
failFast true
