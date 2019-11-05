//semantic commit
def config = [
    owner: 'ksql',
    slackChannel: '#ksql-alerts',
    ksql_db_version: "0.6.0-SNAPSHOT",
    cp_version: "v5.4.0-beta.......",
    release: false,
    revision: 'refs/heads/master',
    dockerRegistry: '368821881613.dkr.ecr.us-west-2.amazonaws.com/',
    dockerRepos: ['confluentinc/ksql-db-cli', 'confluentinc/ksql-db-rest-app']
]

def releaseParams = [
    boolean(name: 'GIT_SHA',
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
        config.revision = params.GIT_SHA
        // For a release build we remove the -SNAPSHOT from the version.
        config.ksql_db_version = config.ksql_db_version.tokenize("-")[0]
        config.docker_tag = config.ksql_db_version
    } else {
        config.docker_tag = config.ksql_db_version.tokenize("-")[0] + '-' + env.BUILD_NUMBER
    }

    stage('Checkout KSQL') {
        checkout changelog: false,
            poll: false,
            scm: [$class: 'GitSCM',
                branches: [[name: revision]],
                doGenerateSubmoduleConfigurations: false,
                extensions: [],
                submoduleCfg: [],
                relativeTargetDir: 'ksql-db',
                userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                    url: 'git@github.com:confluentinc/ksql.git']]
            ]
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
                    withDockerServer([uri: dockerHost()]) {
                        withMaven(globalMavenSettingsConfig: 'jenkins-maven-global-settings', options: [findbugsPublisher(disabled: true)]) {
                            // findbugs publishing is skipped in both steps because multi-module projects cause
                            // extra copies to be reported. Instead, use commonPost to collect once at the end
                            // of the build.
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
                            cmd = "mvn --batch-mode -Pjenkins clean install deploy dependency:analyze site validate -U "
                            cmd += "-Ddocker.skip-build=false "
                            cmd += "-Ddocker.tag=${config.docker_tag}"
                            cmd += "-Ddocker.registry=${config.dockerRegistry}"
                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                sh cmd
                            }
                            step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])
                        }
                    }
                }
        }
    }

    stage('Publish Docker Images') {
        withDockerServer([uri: dockerHost()]) {
            config.dockerRepos.each { dockerRepo ->
                sh "docker tag ${config.dockerRegistry}${dockerRepo}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:master-latest"
                sh "docker push ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}"
                sh "docker push ${config.dockerRegistry}${dockerRepo}:master-latest"

                if (config.release) {
                    sh "docker tag ${config.dockerRegistry}${dockerRepo}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-${env.BUILD_NUMBER}"
                    sh "docker push ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-${env.BUILD_NUMBER}"
                }
            }
        }
    }
}

def post = {
    withDockerServer([uri: dockerHost()]) {
        config.dockerRepos.reverse().each { dockerRepo ->
            sh """#!/usr/bin/env bash \n
            images=\$(docker images -q ${config.dockerRegistry}${dockerRepo}:${config.docker_tag})
            if [[ ! -z \$images ]]; then
                docker rmi -f \$images || true
                else
                    echo 'No images for ${dockerRepo}:${config.docker_tag} need cleanup'
                fi
            """
            }
        }
    }

    commonPost(finalConfig)
}
runJob finalConfig, job, post


