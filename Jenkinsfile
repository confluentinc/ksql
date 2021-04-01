#!/usr/bin/env groovy
import java.util.regex.Pattern

def baseConfig = {
    owner = 'ksql'
    slackChannel = '#ksqldb-warn'
    ksql_db_version = "0.18.0"  // next version to be released
    cp_version = "6.2.0-beta210310224144-cp7"  // must be a beta version from the packaging build
    packaging_build_number = "1"
    default_git_revision = 'refs/heads/master' 
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    altDockerRegistry = 'confluent-docker.jfrog.io/'
    dockerArtifacts = ['confluentinc/ksqldb-docker', 'confluentinc/ksqldb-docker']
    dockerRepos = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
    nodeLabel = 'docker-oraclejdk8-compose-swarm'
    dockerScan = true
    cron = '@daily'
    maven_packages_url = "https://jenkins-confluent-packages-beta-maven.s3-us-west-2.amazonaws.com"
    dockerPullDeps = ['confluentinc/cp-base-new']
    kafka_tutorials_branch = 'ksqldb-latest'
}

def defaultParams = [
    booleanParam(name: 'RELEASE_BUILD',
        description: 'Is this a release build. If so, GIT_REVISION and KSQLDB_VERSION must be specified, and the downstream CCloud job will not be triggered.',
        defaultValue: false),
    string(name: 'GIT_REVISION',
        description: 'The git ref (SHA or branch or tag) to build ksqlDB from.',
        defaultValue: ''),
    booleanParam(name: 'PROMOTE_TO_PRODUCTION',
        defaultValue: false,
        description: 'Promote images to production (DockerHub) for release build images only.'),
    string(name: 'KSQLDB_VERSION',
        defaultValue: '',
        description: 'If PROMOTE_TO_PRODUCTION, ksqlDB version to promote to production. Else, override the ksql_db_version defined in the Jenkinsfile, representing the next version to be released.'),
    booleanParam(name: 'UPDATE_LATEST_TAG',
        defaultValue: false,
        description: 'Should the latest tag on docker hub be updated to point at this new image version. Only relevant if PROMOTE_TO_PRODUCTION is true.'),
    string(name: 'CP_VERSION',
        description: 'the CP version to build ksqlDB against',
        defaultValue: ''),
    string(name: 'CP_BUILD_NUMBER',
        description: 'the packaging build number that published beta artifacts',
        defaultValue: ''),
    string(name: 'CCLOUD_GIT_REVISION',
        description: 'The git ref (SHA or branch or tag) to base the cc-docker-ksql build from.',
        defaultValue: ''),
    string(name: 'CCLOUD_KSQL_BASE_VERSION',
        description: 'The version of the initial cc-ksql RC we are basing our cloud release off of (e.g. v0.15.0-rc123-456)',
        defaultValue: '')
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

    config.ksql_db_packages_prefix = config.ksql_db_version.tokenize('.')[0..1].join('.')

    if (params.CP_VERSION != '') {
        config.cp_version = params.CP_VERSION
    }

    if (params.CP_BUILD_NUMBER != '') {
        config.packaging_build_number = params.CP_BUILD_NUMBER
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
        // For non-release builds, we upload the RC artifacts to a different prefix for testing.
        // Actual release artifacts will go to a "prod" prefix.
        config.ksql_db_packages_prefix = config.ksql_db_packages_prefix + '-rcs'
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

        stage("Promote Artifacts") {
            withDockerServer([uri: dockerHost()]) {
                writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                sh """
                    bash extract-iam-credential.sh
                    aws s3 sync s3://staging-ksqldb-maven/maven/ s3://ksqldb-maven/maven/
                    aws s3 sync s3://staging-ksqldb-packages/rpm/${config.ksql_db_packages_prefix} s3://ksqldb-packages/rpm/${config.ksql_db_packages_prefix}
                    aws s3 sync s3://staging-ksqldb-packages/deb/${config.ksql_db_packages_prefix} s3://ksqldb-packages/deb/${config.ksql_db_packages_prefix}
                    aws s3 sync s3://staging-ksqldb-packages/archive/${config.ksql_db_packages_prefix} s3://ksqldb-packages/archive/${config.ksql_db_packages_prefix}
                """
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
                                
                                // we want to make sure that any dependency we use to build an image is backed up 
                                // the artifactory repo (which is not ephemeral, like ECR)
                                sh "docker tag ${config.dockerRegistry}${dockerRepo}:${config.cp_version}-latest ${config.altDockerRegistry}${dockerRepo}:${config.cp_version}-latest"
                                sh "docker push ${config.altDockerRegistry}${dockerRepo}:${config.cp_version}-latest"
                            }

                            // Install utilities required for building. XXX: Add to base image
                            sh """
                                sudo apt update
                                sudo apt install -y devscripts git-buildpackage dh-systemd javahelper xmlstarlet
                            """
                            // Copy settingsFile into the Jenkin's user's maven config, because when we build debian packages
                            // through the debian wrapper scripts, it will not honor the Jenkinsfile withMaven(globalMavenSettingsFilePath..)
                            // settings. Version setting & build logic is contained within the build-packages.sh script.
                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                sh """
                                cp ${settingsFile} ~/.m2/settings.xml
                                cp ${settingsFile} .
                                ${env.WORKSPACE}/build-packages.sh --workspace . \
                                    --docker-registry ${config.dockerRegistry} \
                                    --project-version ${config.ksql_db_artifact_version} \
                                    --upstream-version ${config.cp_version} --jar
                                
                                """
                            }
                            // Run smoke tests for the packages produced above
                            sh """
                                echo "Package Smoke tests"
                                ${env.WORKSPACE}/smoke/run_smoke.sh ${env.WORKSPACE} ./output/    
                            """
                            step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])

                            if (!config.isPrJob) {
                                def git_tag = "v${config.ksql_db_artifact_version}-ksqldb"
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
        stage('Publish Artifacts') {
            writeFile file: settingsFile, text: settings
            dir('ksql-db') {
                def gpg_packaging_key = ''
                gpg_packaging_key = setupSSHKey("gpg/packaging", "private_key", "${env.WORKSPACE}/confluent-packaging-private.key")
                withVaultEnv([
                    ["artifactory/jenkins_access_token", "user", "ARTIFACTORY_USERNAME"],
                    ["artifactory/jenkins_access_token", "access_token", "ARTIFACTORY_PASSWORD"],
                    ["gpg/packaging", "passphrase", "GPG_PASSPHRASE"]]) {
                    withDockerServer([uri: dockerHost()]) {
                        withMaven(globalMavenSettingsFilePath: settingsFile, options: mavenOptions) {
                            writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')
                            writeFile file:'confluent-process-packages.sh', text:libraryResource('scripts/confluent-process-packages.sh')
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
                            sh """
                                set +x
                                TMP_GPG_PASS=\$(mktemp -t XXXgpgpass)
                                echo "\${GPG_PASSPHRASE}" > "\${TMP_GPG_PASS}"
                                bash confluent-process-packages.sh \
                                    --bucket staging-ksqldb-packages \
                                    --region us-west-2 \
                                    --prefix '\$PACKAGE_TYPE/${config.ksql_db_packages_prefix}' \
                                    --input-dir "\${PWD}/output" \
                                    --sign-key-file "${gpg_packaging_key}" \
                                    --passphrase-file "\${TMP_GPG_PASS}"
                                rm -f "\${TMP_GPG_PASS}"
                            """
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
                    string(name: "KSQLDB_ARTIFACT_VERSION", value: "${config.ksql_db_artifact_version}"),
                    string(name: "CCLOUD_GIT_REVISION", value: "${params.CCLOUD_GIT_REVISION}"),
                    string(name: "CCLOUD_KSQL_BASE_VERSION", value: "${params.CCLOUD_KSQL_BASE_VERSION}")
                ]
        }
    }

    // Kick off automated Kafka Tutorials test run

    if (!config.isPrJob) {
        stage('checkout kafka tutorials') {
            checkout changelog: false,
                poll: false,
                scm: [$class: 'GitSCM',
                    branches: [[name: "${config.kafka_tutorials_branch}"]],
                    doGenerateSubmoduleConfigurations: false,
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'kafka-tutorials']],
                    submoduleCfg: [],
                    userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                        url: 'git@github.com:confluentinc/kafka-tutorials.git']]
                ]
        }

        stage('update kafka tutorials and kick off semaphore test') {
            dir('kafka-tutorials') {
                withVaultEnv([["artifactory/jenkins_access_token", "user", "ARTIFACTORY_USERNAME"],
                    ["artifactory/jenkins_access_token", "access_token", "ARTIFACTORY_PASSWORD"],
                    ["github/confluent_jenkins", "user", "GIT_USER"],
                    ["github/confluent_jenkins", "access_token", "GIT_TOKEN"]]) {
                    withEnv(["GIT_CREDENTIAL=${env.GIT_USER}:${env.GIT_TOKEN}"]) {
                        sh "./tools/update-ksqldb-version.sh ${config.ksql_db_artifact_version} ${config.dockerRegistry}"
                        // run git diff in order to help debug if something goes wrong
                        sh "git diff"
                        sh "git add _includes/*"
                        sh "git commit --allow-empty -m \"build: set ksql version to ${config.ksql_db_artifact_version}\""
                        sshagent (credentials: ['ConfluentJenkins Github SSH Key']) {
                            sh "git push origin HEAD:${config.kafka_tutorials_branch}"
                        }
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
    // Remove any images we created during a smoke test
    sh '''
        docker rmi --force ksqldb-package-test-deb || true
    '''
    commonPost(config)
}

runJob config, job, post
