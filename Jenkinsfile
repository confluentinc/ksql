#!/usr/bin/env groovy

parallel unitTests: { // Skip the integration tests.  The unit tests take longer the the ITs, so we push the spotbugs and Maven site goals to the other job.
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -Dspotbugs.skip -Dmaven.site.skip"
    }
}, integrationTests: { // Skip the unit tests, run the ITs and the rest of the build.
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
        extraBuildArgs = "-Dmaven.gitcommitid.nativegit=true -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipTests"
    }
}
failFast: true
