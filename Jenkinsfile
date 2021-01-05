import java.util.regex.Pattern

def baseConfig = {
    owner = 'ksql'
    slackChannel = '#ksql-alerts'
    ksql_db_version = "0.15.0"  // next version to be released
    cp_version = "6.2.0-beta201122193350"  // must be a beta version from the packaging build
    packaging_build_number = "1"
    default_git_revision = 'refs/heads/master'
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    dockerArtifacts = ['confluentinc/ksqldb-docker', 'confluentinc/ksqldb-docker']
    dockerRepos = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
    nodeLabel = 'docker-oraclejdk8-compose-swarm'
    dockerScan = true
    cron = '@daily'
    maven_packages_url = "https://jenkins-confluent-packages-beta-maven.s3-us-west-2.amazonaws.com"
    dockerPullDeps = ['confluentinc/cp-base-new']
}

def defaultParams = [
    booleanParam(name: 'RELEASE_BUILD',
        description: 'Is this a release build. If so, GIT_REVISION and KSQLDB_VERSION must be specified, and the downstream CCloud job will not be triggered.',
        defaultValue: false),
    string(name: 'GIT_REVISION',
        description: 'The git SHA to build ksqlDB from.',
        defaultValue: ''),
    booleanParam(name: 'PROMOTE_TO_PRODUCTION',
        defaultValue: false,
        description: 'Promote images to production (DockerHub) for release build images only.'),
    string(name: 'KSQLDB_VERSION',
        defaultValue: '',
        description: 'If PROMOTE_TO_PRODUCTION, ksqlDB version to promote to production. Else, override the ksql_db_version defined in the Jenkinsfile, representing the next version to be released.'),
    booleanParam(name: 'UPDATE_LATEST_TAG',
        defaultValue: false,
        description: 'Should the latest tag on docker hub be updated to point at this new image version. Only relevant if PROMOTE_TO_PRODUCTION is true.')
]

def updateConfig = { c ->
    c.properties = [parameters(defaultParams)]
}

def config = jobConfig(baseConfig, [:], updateConfig)

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

    if (params.RELEASE_BUILD && params.KSQLDB_VERSION == '') {
        currentBuild.result = 'ABORTED'
        error("You must provide the KSQLDB_VERSION when doing a release build.")
    }

    if (params.PROMOTE_TO_PRODUCTION && params.KSQLDB_VERSION == '') {
        currentBuild.result = 'ABORTED'
        error("You must provide the KSQLDB_VERSION when promoting images to docker hub.")
    }

    config.release = params.RELEASE_BUILD

    // Use ksqlDB version param if provided
    if (params.KSQLDB_VERSION != '') {
        config.ksql_db_version = params.KSQLDB_VERSION
    }

    if (config.release) {
        // For a release build check out the provided git sha instead of the master branch.
        if (config.revision == '') {
            error("If you are doing a release build you must provide a git sha.")
        }

        config.ksql_db_artifact_version = config.ksql_db_version
    } else {
        // For non-release builds, we append the build number to the maven artifacts and docker image tag
        config.ksql_db_artifact_version = config.ksql_db_version + '-rc' + env.BUILD_NUMBER
    }
    config.docker_tag = config.ksql_db_artifact_version
    
    // Use revision param if provided, otherwise default to master
    config.revision = params.GIT_REVISION ?: config.default_git_revision

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
        stage("Pulling Docker Images") {
            withVaultEnv([["artifactory/jenkins_access_token", "user", "ARTIFACTORY_USERNAME"],
                ["artifactory/jenkins_access_token", "access_token", "ARTIFACTORY_PASSWORD"]]) {
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
        }

        stage("Tag and Push Images") {
            withVaultEnv([["docker_hub/jenkins", "user", "DOCKER_USERNAME"],
                ["docker_hub/jenkins", "password", "DOCKER_PASSWORD"]]) {
                withDockerServer([uri: dockerHost()]) {
                    config.dockerRepos.each { dockerRepo ->
                        sh '''
                            set +x
                            echo $DOCKER_PASSWORD | docker login --username $DOCKER_USERNAME --password-stdin
                        '''
                        sh "docker tag ${config.dockerRegistry}${dockerRepo}:${params.KSQLDB_VERSION} ${dockerRepo}:${params.KSQLDB_VERSION}"
                        sh "docker push ${dockerRepo}:${params.KSQLDB_VERSION}"

                        if (params.UPDATE_LATEST_TAG) {
                            sh "docker tag ${dockerRepo}:${params.KSQLDB_VERSION} ${dockerRepo}:latest"
                            sh "docker push ${dockerRepo}:latest"
                        }
                    }
                }
            }
        }

        stage("Promote Maven Artifacts") {
            withDockerServer([uri: dockerHost()]) {
                writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                sh "bash extract-iam-credential.sh"
                sh "aws s3 sync s3://staging-ksqldb-maven/maven/ s3://ksqldb-maven/maven/"
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
            withVaultEnv([["artifactory/jenkins_access_token", "user", "ARTIFACTORY_USERNAME"],
                ["artifactory/jenkins_access_token", "access_token", "ARTIFACTORY_PASSWORD"],
                ["github/confluent_jenkins", "user", "GIT_USER"],
                ["github/confluent_jenkins", "access_token", "GIT_TOKEN"]]) {
                withEnv(["GIT_CREDENTIAL=${env.GIT_USER}:${env.GIT_TOKEN}"]) {
                    withDockerServer([uri: dockerHost()]) {
                        withMaven(globalMavenSettingsFilePath: settingsFile, options: mavenOptions) {
                            writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                            writeFile file:'create-pip-conf-with-jfrog.sh', text:libraryResource('scripts/create-pip-conf-with-jfrog.sh')
                            writeFile file:'create-pypirc-with-jfrog.sh', text:libraryResource('scripts/create-pypirc-with-jfrog.sh')
                            writeFile file:'setup-credential-store.sh', text:libraryResource('scripts/setup-credential-store.sh')
                            writeFile file:'set-global-user.sh', text:libraryResource('scripts/set-global-user.sh')
                            sh '''
                                bash extract-iam-credential.sh

                                # Hide login credential from below
                                set +x

                                LOGIN_CMD=$(aws ecr get-login --no-include-email --region us-west-2)
                                $LOGIN_CMD

                                echo $ARTIFACTORY_PASSWORD | docker login confluent-docker.jfrog.io -u $ARTIFACTORY_USERNAME --password-stdin

                                set -x
                                bash create-pip-conf-with-jfrog.sh
                                bash create-pypirc-with-jfrog.sh
                                bash setup-credential-store.sh
                                bash set-global-user.sh
                            '''

                            config.dockerPullDeps.each { dockerRepo ->
                                sh "docker pull ${config.dockerRegistry}${dockerRepo}:${config.cp_version}-latest"
                            }

                            // We need to replace the parent version range before we can run any maven commands
                            def pomFile = readFile('pom.xml')
                            def parentVersionPattern = Pattern.compile(/(.*<parent>.*<groupId>io.confluent\S*<\/groupId>.*<version>)\[\d+\.\d+\.\d+-\d+\,\s+\d+\.\d+\.\d+-\d+\)(<\/version>.*<\/parent>.*)/, Pattern.DOTALL)
                            // Groovy regex replaces the groups we didn't match, so we print the beginning of the file, our version, and the rest of the file.
                            def newPomFile = pomFile.replaceFirst(parentVersionPattern, "\$1${config.cp_version}\$2")
                            writeFile(file: 'pom.xml', text: newPomFile)

                            // Set the project versions in the pom files
                            sh "set -x"
                            sh "mvn --batch-mode versions:set -DnewVersion=${config.ksql_db_artifact_version} -DgenerateBackupPoms=false"

                            // Set the repo version property
                            sh "mvn --batch-mode versions:set-property -DgenerateBackupPoms=false -DnewVersion=${config.ksql_db_artifact_version} -Dproperty=io.confluent.ksql.version"

                            // Set the version of schema-registry to use
                            sh "mvn --batch-mode versions:set-property -DgenerateBackupPoms=false -DnewVersion=${config.cp_version} -Dproperty=io.confluent.schema-registry.version"

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

                            sh "cp ${settingsFile} ."

                            if (!config.isPrJob) {
                                def git_tag = "v${config.ksql_db_artifact_version}-ksqldb"
                                sh "git add ."
                                sh "git commit -m \"build: Setting project version ${config.ksql_db_artifact_version} and parent version ${config.cp_version}.\""
                                sh "git tag ${git_tag}"
                                sshagent (credentials: ['ConfluentJenkins Github SSH Key']) {
                                    sh "git push origin ${git_tag}"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if (!config.isPrJob) {
        stage('Publish Maven Artifacts') {
            writeFile file: settingsFile, text: settings
            dir('ksql-db') {
                withVaultEnv([["artifactory/jenkins_access_token", "user", "ARTIFACTORY_USERNAME"],
                    ["artifactory/jenkins_access_token", "access_token", "ARTIFACTORY_PASSWORD"]]) {
                    withDockerServer([uri: dockerHost()]) {
                        withMaven(globalMavenSettingsFilePath: settingsFile, options: mavenOptions) {
                            writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                            sh '''
                                bash extract-iam-credential.sh
                            '''
                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                cmd = "mvn --batch-mode -Pjenkins deploy -DskipTests -Ddocker.skip-build=true -Ddocker.skip-test=true"
                                cmd += " -DaltDeploymentRepository=confluent-artifactory-central::default::s3://staging-ksqldb-maven/maven"
                                cmd += " -DrepositoryId=confluent-artifactory-central"
                                cmd += " -DnexusUrl=s3://staging-ksqldb-maven/maven"
                                sh cmd
                            }
                        }
                    }
                }
            }
        }
    }

    if (!config.isPrJob) {
        stage('Rename ksqlDB Docker Images') {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.eachWithIndex { dockerRepo, index ->
                    dockerArtifact = config.dockerArtifacts[index]
                    sh "docker tag ${config.dockerRegistry}${dockerArtifact}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}"
                }
            }
        }
    }

    if(config.dockerScan && !config.isPrJob){
        stage('Twistloc scan') {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.each { dockerRepo ->
                                            dockerName = config.dockerRegistry+dockerRepo+":${config.docker_tag}"
                                            echo "Twistloc Scan ${dockerName}"
                                            prismaCloudScanImage  ca: '',
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

    if(config.dockerScan && !config.isPrJob){
        stage('Twistloc publish') {
            withDockerServer([uri: dockerHost()]) {
                config.dockerRepos.each { dockerRepo ->
                                            dockerName = config.dockerRegistry+dockerRepo+":${config.docker_tag}"
                                            echo "Twistloc Publish ${dockerName}"
                                            prismaCloudPublish  ca: '',
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
                        // docker_tag for release builds does not include the build number
                        // here we add it back so an image version with the build number always exists
                        sh "docker tag ${config.dockerRegistry}${dockerRepo}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-rc${env.BUILD_NUMBER}"
                        sh "docker push ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-rc${env.BUILD_NUMBER}"
                    }
                }
            }
        }
    }

    // Build CCloud ksqlDB image

    if (!config.release && !config.isPrJob) {
        stage('Trigger CCloud ksqlDB Docker image job') {
            build job: "confluentinc/cc-docker-ksql/master",
                wait: false,
                parameters: [
                    booleanParam(name: "NIGHTLY_BUILD", value: true),
                    string(name: "NIGHTLY_BUILD_NUMBER", value: "${env.BUILD_NUMBER}"),
                    string(name: "CP_BETA_VERSION", value: "${config.cp_version}"),
                    string(name: "CP_BETA_BUILD_NUMBER", value: "${config.packaging_build_number}"),
                    string(name: "KSQLDB_ARTIFACT_VERSION", value: "${config.ksql_db_artifact_version}")
                ]
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
    commonPost(config)
}

runJob config, job, post
