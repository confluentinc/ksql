
def config = {
    owner = 'ksql'
    slackChannel = '#ksqldb-alerts'
    ksql_db_version = "0.6.0-SNAPSHOT"
    cp_version = "5.5.0-beta191113214629"
    packaging_build_number = "1"
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    dockerArtifacts = ['confluentinc/ksql-cli', 'confluentinc/ksql-rest-app']
    dockerRepos = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
    nodeLabel = 'docker-oraclejdk8-compose-swarm'
    dockerScan = true
    cron = '@daily'
    maven_packages_url = "https://jenkins-confluent-packages-beta-maven.s3-us-west-2.amazonaws.com"
    dockerPullDeps = ['confluentinc/cp-base-new']
}

def defaultParams = [
    booleanParam(name: 'RELEASE_BUILD',
        description: 'Is this a release build.',
        defaultValue: false),
    string(name: 'GIT_REVISION',
        description: 'The git SHA to create the release build from.',
        defaultValue: ''),
    booleanParam(name: 'PROMOTE_TO_PRODUCTION',
        defaultValue: false,
        description: 'Promote images to production (DockerHub) for release build images only.'),
    string(name: 'KSQLDB_VERSION',
        defaultValue: '',
        description: 'KSQLDB version to promote to production.'),
    string(name: 'IMAGE_REVISION',
        defaultValue: '1',
        description: 'Revision for Docker images. This is used with PROMOTE_TO_PRODUCTION only'),
    booleanParam(name: 'UPDATE_LATEST_TAG',
        defaultValue: false,
        description: 'Should the latest tag on docker hub be updated to point at this new image version.')
]

def updateConfig = { c ->
    c.properties = [parameters(defaultParams)]
}

def finalConfig = jobConfig(config, [:], updateConfig)

def job = {
    if (config.isPrJob && params.PROMOTE_TO_PRODUCTION) {
        currentBuild.result = 'ABORTED'
        error('PROMOTE_TO_PRODUCTION can not be used with PR builds.')
    }

    if (config.isPrJob && params.RELEASE_BUILD) {
        currentBuild.result = 'ABORTED'
        error('RELEASE_BUILD can not be used with PR builds.')
    }

    if (params.RELEASE_BUILD && params.PROMOTE_TO_PRODUCTION) {
        currentBuild.result = 'ABORTED'
        error("You can not do a release build and promote to production at the same time.")
    }

    if (params.RELEASE_BUILD && params.GIT_REVISION == '') {
        currentBuild.result = 'ABORTED'
        error("You must provide the GIT_REVISION when doing a release build.")
    }

    if (params.PROMOTE_TO_PRODUCTION && params.KSQLDB_VERSION == '') {
        currentBuild.result = 'ABORTED'
        error("You must provide the KSQLDB_VERSION when promoting images to docker hub.")
    }

    config.release = params.RELEASE_BUILD

    if (config.release) {
        // For a release build check out the provided git sha instead of the master branch.
        if (config.revision == '') {
            error("If you are doing a release build you must provide a git sha.")
        }
        
        config.revision = params.GIT_REVISION
        // For a release build we remove the -SNAPSHOT from the version.
        config.ksql_db_version = config.ksql_db_version.tokenize("-")[0]
        config.docker_tag = config.ksql_db_version
    } else {
        config.docker_tag = config.ksql_db_version.tokenize("-")[0] + '-' + env.BUILD_NUMBER
        config.revision = 'refs/heads/master'
    }

    // Configure the maven repo settings so we can download from the beta artifacts repo
    def settingsFile = "${env.WORKSPACE}/maven-settings.xml"
    def maven_packages_url = "${config.maven_packages_url}/${config.cp_version}/${config.packaging_build_number}/maven"
    def settings = readFile('maven-settings-template.xml').replace('PACKAGES_MAVEN_URL', maven_packages_url)
    writeFile file: settingsFile, text: settings
    mavenOptions = [artifactsPublisher(disabled: true),
        junitPublisher(disabled: true),
        openTasksPublisher(disabled: true)
    ]

    if (params.PROMOTE_TO_PRODUCTION) {
        withCredentials([usernamePassword(credentialsId: 'JenkinsArtifactoryAccessToken', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_USERNAME')]) {
            withDockerServer([uri: dockerHost()]) {
                writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                sh '''
                    bash extract-iam-credential.sh

                    # Hide login credential from below
                    set +x

                    LOGIN_CMD=$(aws ecr get-login --no-include-email --region us-west-2)

                    $LOGIN_CMD
                '''
                config.dockerRepos.each { dockerRepo ->
                    sh "docker pull ${config.dockerRegistry}${dockerRepo}:${params.KSQLDB_VERSION}"
                }
            }
        }

        dockerHubCreds = usernamePassword(
            credentialsId: 'Jenkins Docker Hub Account',
            passwordVariable: 'DOCKER_PASSWORD',
            usernameVariable: 'DOCKER_USERNAME')
        withCredentials([dockerHubCreds]) {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.each { dockerRepo ->
                    sh "docker login --username $DOCKER_USERNAME --password \'$DOCKER_PASSWORD\'"
                    sh "docker tag ${config.dockerRegistry}${dockerRepo}:${params.KSQLDB_VERSION} ${dockerRepo}:${params.KSQLDB_VERSION}-${params.IMAGE_REVISION}"
                    sh "docker push ${dockerRepo}:${params.KSQLDB_VERSION}-${params.IMAGE_REVISION}"

                    if (params.UPDATE_LATEST_TAG) {
                        sh "docker tag ${dockerRepo}:${params.KSQLDB_VERSION}-${params.IMAGE_REVISION} ${dockerRepo}:latest"
                        sh "docker push ${dockerRepo}:latest"
                    }
                }
            }
        }
        return null
    }

    stage('Checkout KSQL') {
        checkout changelog: false,
            poll: false,
            scm: [$class: 'GitSCM',
                branches: [[name: config.revision]],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'ksql-db']],
                submoduleCfg: [],
                userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                    url: 'git@github.com:confluentinc/ksql.git']]
            ]
    }

    stage('Build KSQL-DB') {
        dir('ksql-db') {
            archiveArtifacts artifacts: 'pom.xml'
            withCredentials([
                usernamePassword(credentialsId: 'Jenkins Nexus Account', passwordVariable: 'NEXUS_PASSWORD', usernameVariable: 'NEXUS_USERNAME'),
                usernamePassword(credentialsId: 'JenkinsArtifactoryAccessToken', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_USERNAME'),
                usernameColonPassword(credentialsId: 'Jenkins GitHub Account', variable: 'GIT_CREDENTIAL')]) {
                    withDockerServer([uri: dockerHost()]) {
                        withMaven(globalMavenSettingsFilePath: settingsFile, options: mavenOptions) {
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

                            config.dockerPullDeps.each { dockerRepo ->
                                sh "docker pull ${config.dockerRegistry}${dockerRepo}:${config.cp_version}-latest"
                            }

                            // Set the project versions in the pom files
                            sh "set -x"
                            sh "mvn --batch-mode versions:set -DnewVersion=${config.ksql_db_version} -DgenerateBackupPoms=false"

                            // Set the version of the parent project to use.
                            sh "mvn --batch-mode versions:update-parent -DparentVersion=\"[${config.cp_version}]\" -DgenerateBackupPoms=false"

                            if (config.release) {
                                def git_tag = "v${config.ksql_db_version}-ksqldb"
                                sh "git add ."
                                sh "git commit -m \"build: Setting project version ${config.ksql_db_version} and parent version ${config. cp_version}.\""
                                sh "git tag ${git_tag}"
                                sshagent (credentials: ['ConfluentJenkins Github SSH Key']) {
                                    sh "git push origin ${git_tag}"
                                }
                            }

                            cmd = "mvn --batch-mode -Pjenkins clean package dependency:analyze site validate -U "
                            cmd += "-DskipTests "
                            cmd += "-Dspotbugs.skip "
                            cmd += "-Dcheckstyle.skip "
                            cmd += "-Ddocker.tag=${config.docker_tag} "
                            cmd += "-Ddocker.registry=${config.dockerRegistry} "
                            cmd += "-Ddocker.upstream-tag=${config.cp_version}-latest "
                            cmd += "-Dskip.docker.build=false "

                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                sh cmd
                            }
                            step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])
                        }
                    }
                }
        }
    }

    if (!config.isPrJob) {
            stage('Rename Maven Docker Images') {
                withDockerServer([uri: dockerHost()]) {
                    config.dockerRepos.eachWithIndex { dockerRepo, index ->
                        dockerArtifact = config.dockerArtifacts[index]
                        sh "docker tag ${config.dockerRegistry}${dockerArtifact}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}"
                    }
                }
            }
        }

    if(config.dockerScan){
        stage('Twistloc scan') {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.each { dockerRepo ->
                                            dockerName = config.dockerRegistry+dockerRepo+":${config.docker_tag}"
                                            echo "Twistloc Scan ${dockerName}"
                                            twistlockScan   ca: '',
                                                            cert: '',
                                                            compliancePolicy: 'critical',
                                                            dockerAddress: dockerHost(),
                                                            gracePeriodDays: 0,
                                                            ignoreImageBuildTime: true,
                                                            image: dockerName,
                                                            key: '',
                                                            logLevel: 'true',
                                                            policy: 'warn',
                                                            requirePackageUpdate: false,
                                                            timeout: 10
                }
            }
        }
    }

    if(config.dockerScan){
        stage('Twistloc publish') {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.each { dockerRepo ->
                                            dockerName = config.dockerRegistry+dockerRepo+":${config.docker_tag}"
                                            echo "Twistloc Publish ${dockerName}"
                                            twistlockPublish    ca: '',
                                                                cert: '',
                                                                dockerAddress: dockerHost(),
                                                                ignoreImageBuildTime: true,
                                                                image: dockerName,
                                                                key: '',
                                                                logLevel: 'true',
                                                                timeout: 10
                }
            }
        }
    }

    if (!config.isPrJob) {
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
}

def post = {
    withDockerServer([uri: dockerHost()]) {
        repos = config.dockerArtifacts + config.dockerRepos
        repos.reverse().each { dockerRepo ->
            if (params.PROMOTE_TO_PRODUCTION) {
                sh """#!/usr/bin/env bash \n
                images=\$(docker images -q ${dockerRepo})
                if [[ ! -z \$images ]]; then
                    docker rmi -f \$images || true
                    else
                        echo 'No images for ${dockerRepo} need cleanup'
                    fi
                """
            }
            sh """#!/usr/bin/env bash \n
            images=\$(docker images -q ${config.dockerRegistry}${dockerRepo})
            if [[ ! -z \$images ]]; then
                docker rmi -f \$images || true
                else
                    echo 'No images for ${config.dockerRegistry}${dockerRepo} need cleanup'
                fi
            """
        }
    }
    commonPost(finalConfig)
}

runJob finalConfig, job, post
