
def config = [
    owner: "ksql",
    slackChannel: 'ksql-notifications',
    ksql_db_version: "0.6.0-SNAPSHOT",
    cp_version: "v5.4.0-beta.......",
    release: false,
    revision: 'refs/heads/master',
    dockerRegistry: '368821881613.dkr.ecr.us-west-2.amazonaws.com/',

]

def releaseParams = [
    string(name: 'GIT_SHA',
        description: 'The git SHA to create the release build from.')
]

def finalConfig = jobConfig(null, config, { c ->
    if (c.release) {
        c.properties = [parameters(releaseParams)]
    }
})

def job = {
    if (config.release) {
        // For a release build check out the provided git sha instead of the master branch.
        config.revision = ${params.GIT_SHA}
        // For a release build we remove the -SNAPSHOT from the version.
        config.ksql_db_version = config.ksql_db_version.tokenize("-")[0]
    }

    stage('Checkout KSQL') {
        checkout changelog: false,
            poll: false,
            scm: [$class: 'GitSCM',
                branches: [[name: revision]],
                doGenerateSubmoduleConfigurations: false,
                extensions: [],
                submoduleCfg: [],
                relativeTargetDir: 'ksql-db'
                userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                    url: 'git@github.com:confluentinc/ksql.git']]]
    }

    stage('Set Project Version') {
        dir('ksql-db') {
            // Set the project versions in the pom files
            sh "mvn versions:set -DnewVersion=${config.ksql_db_version} -DgenerateBackupPoms=false"
        }
    }

    stage('Set Parent Version') {
        dir('ksql-db') {
            // Set the version of the parent project to use.
            sh "mvn versions:update-parent -DparentVersion=\"[${config.cp_version}]\" -DgenerateBackupPoms=false"
        }
    }

    if (config.release) {
        stage('Tag Repo') {
            dir('ksql-db') {
                sh "git add ."
                sh "git commit -m \"Setting project version and parent version.\""
                sh "git tag ${config.ksql_db_version}"
                sh "git push -f origin ${config.ksql_db_version}"
            }
        }
    }

    stage('Build KSQL-DB') {
        dir('ksql-db') {
            archiveArtifacts artifacts: 'pom.xml'
            withCredentials([
                usernamePassword(credentialsId: 'Jenkins Nexus Account', passwordVariable: 'NEXUS_PASSWORD', usernameVariable: 'NEXUS_USERNAME'),
                usernamePassword(credentialsId: 'JenkinsArtifactoryAccessToken', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_USERNAME'),
                usernameColonPassword(credentialsId: 'Jenkins GitHub Account', variable: 'GIT_CREDENTIAL')]) {
                    config.gitCommit = sh(script: "git rev-parse --verify HEAD --short", returnStdout: true).trim()

                    withDockerServer([uri: dockerHost()]) {
                        withMaven(
                            globalMavenSettingsConfig: 'jenkins-maven-global-settings',
                            // findbugs publishing is skipped in both steps because multi-module projects cause
                            // extra copies to be reported. Instead, use commonPost to collect once at the end
                            // of the build.
                            options: [findbugsPublisher(disabled: true)]
                        ) {
                            writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')

                            sh '''
                                bash extract-iam-credential.sh

                                # Hide login credential from below
                                set +x

                                LOGIN_CMD=$(aws ecr get-login --no-include-email --region us-west-2)

                                $LOGIN_CMD
                            '''

                            sh '''
                                echo $ARTIFACTORY_PASSWORD | docker login confluent-docker.jfrog.io -u $ARTIFACTORY_USERNAME --password-stdin
                            '''

                            writeFile file:'create-pip-conf-with-nexus.sh', text:libraryResource('scripts/create-pip-conf-with-nexus.sh')
                            writeFile file:'create-pypirc-with-nexus.sh', text:libraryResource('scripts/create-pypirc-with-nexus.sh')
                            writeFile file:'setup-credential-store.sh', text:libraryResource('scripts/setup-credential-store.sh')
                            writeFile file:'set-global-user.sh', text:libraryResource('scripts/set-global-user.sh')

                            sh '''
                                bash create-pip-conf-with-nexus.sh
                                bash create-pypirc-with-nexus.sh
                                bash setup-credential-store.sh
                                bash set-global-user.sh
                            '''

                            cmd = "mvn --batch-mode -Pjenkins clean ${config.mvnPhase} "
                            cmd += "dependency:analyze site validate -U "
                            cmd += "-Ddocker.registry=${config.dockerRegistry} "
                            cmd += "-Ddocker.tag=${env.BRANCH_NAME}-${env.BUILD_NUMBER} "
                            // cmd += "-Ddocker.upstream-registry=${config.dockerUpstreamRegistry} "
                            // cmd += "-Ddocker.upstream-tag=${config.dockerUpstreamTag} "
                            cmd += "-DBUILD_NUMBER=${env.BUILD_NUMBER} "
                            cmd += "-DGIT_COMMIT=${config.gitCommit}"

                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                sh cmd
                            }

                            step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])
        }
    }
}

def post = {}

runJob finalConfig, job, post


