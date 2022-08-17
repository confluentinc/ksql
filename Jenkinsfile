#!/usr/bin/env groovy
def nightlyConfig = {
    slackChannel = '#ksqldb-quality-oncall'
    nodeLabel = 'docker-debian-jdk8'
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
    nanoVersion = true
    timeoutHours = 3
    upstreamProjects = 'confluentinc/schema-registry'
    extraBuildArgs = ''
}

def update_pr_version(config, repo_name) {
    stage("update pr version") {
        archiveArtifacts artifacts: 'pom.xml'
        withVaultEnv([["artifactory/tools_jenkins", "user", "TOOLS_ARTIFACTORY_USER"],
            ["artifactory/tools_jenkins", "password", "TOOLS_ARTIFACTORY_PASSWORD"]]) {
            withVaultEnv(config.secret_env_list) {
                withDockerServer([uri: dockerHost()]) {
                    def mavenSettingsFile = "${env.WORKSPACE_TMP}/maven-global-settings.xml"
                    withMavenSettings("maven/jenkins_maven_global_settings", "settings", "MAVEN_GLOBAL_SETTINGS", mavenSettingsFile) {
                        withMaven(globalMavenSettingsFilePath: mavenSettingsFile) {
                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                withVaultFile(config.secret_file_list) {
                                    if (config.nanoVersion && !config.isReleaseJob) {
                                        def updateVersionCommand =
                                                "ci-update-version ${env.WORKSPACE} ${repo_name}"

                                        if (config.pinnedNanoVersions) {
                                            updateVersionCommand += " --pinned-nano-versions"
                                        }

                                        ciTool(updateVersionCommand, config.isPrJob)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


def mavenTest(String stage_name, String command, config) {
    stage(stage_name) {
        archiveArtifacts artifacts: 'pom.xml'
        withVaultEnv([["artifactory/tools_jenkins", "user", "TOOLS_ARTIFACTORY_USER"],
            ["artifactory/tools_jenkins", "password", "TOOLS_ARTIFACTORY_PASSWORD"]]) {
            withVaultEnv(config.secret_env_list) {
                withDockerServer([uri: dockerHost()]) {
                    def mavenSettingsFile = "${env.WORKSPACE_TMP}/maven-global-settings.xml"
                    withMavenSettings("maven/jenkins_maven_global_settings", "settings", "MAVEN_GLOBAL_SETTINGS", mavenSettingsFile) {
                        withMaven(globalMavenSettingsFilePath: mavenSettingsFile){
                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                withVaultFile(config.secret_file_list) {
                                    sh """
                                        ${command}
                                    """
                                }
                            }
                        }
                    }
                }
            }
        }
        step([$class: 'DependencyCheckPublisher'])
    }
}
def config = jobConfig nightlyConfig
def job = {
    def maven_command = sh(script: """if test -f "${env.WORKSPACE}/mvnw"; then echo "${env.WORKSPACE}/mvnw"; else echo "mvn"; fi""", returnStdout: true).trim()
    def repo_name = scm.getUserRemoteConfigs()[0].getUrl().tokenize('/').last().split("\\.")[0]
    update_pr_version(config, repo_name)

    def target_list = [
        ["allButUnittests", "${maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins  -Ddocker.skip=true -Pprofile -DprofileFormat=CONSOLE -DskipITs -DskipTests clean verify install"],
        ["integrationTests", "${maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipTests -Dspotbugs.skip -Dmaven.site.skip clean verify install"],
        ["unittestsExcludeRestAndFunctional", "${maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -Dspotbugs.skip -Dmaven.site.skip '-Dtest=!%regex[.*.ksql.rest.*],!%regex[.*.ksql.test.*]' -DfailIfNoTests=false clean verify install"],
        ["unittestsIncludeFunctional", "${maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins  -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.test.*]' -DfailIfNoTests=false clean verify install"],
        ["unittestsIncludeRest", "${maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins  -Ddocker.skip=true -Dprofile -DprofileFormat=CONSOLE -Dmaven.gitcommitid.skip=true -Dassembly.skipAssembly -Dmaven.artifact.threads=16 -DskipITs -DexcludedGroups=io.confluent.common.utils.IntegrationTest -Dspotbugs.skip -Dmaven.site.skip '-Dtest=%regex[.*.ksql.rest.*]' -DfailIfNoTests=false clean verify install"]
    ]

    def run_build_targets = target_list.collectEntries {target -> [target[0], {
        mavenTest(target[0], target[1], config)
    }]}

    run_build_targets.failFast = true
    parallel(run_build_targets)

    mavenTest("Deploy", "{maven_command} ${config.extraBuildArgs} -Dmaven.wagon.http.retryHandler.count=3 --batch-mode -Pjenkins deploy -DskipTests")
}
runJob config, job, { commonPost(config) }