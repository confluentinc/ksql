def config = {
    owner = 'ksql'
    slackChannel = ''
    ksql_db_version = "0.8.0-SNAPSHOT"
    cp_version = "6.0.0-beta200310175819"
    packaging_build_number = "1"
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    dockerArtifacts = ['confluentinc/ksqldb-docker', 'confluentinc/ksqldb-docker']
    dockerRepos = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']
    nodeLabel = 'docker-oraclejdk8-compose-swarm'
    dockerScan = true
    cron = '@daily'
    maven_packages_url = "https://jenkins-confluent-packages-beta-maven.s3-us-west-2.amazonaws.com"
    dockerPullDeps = ['confluentinc/cp-base-new']

    ccloudDockerRepo = 'confluentinc/cc-ksql'
    ccloudDockerUpstreamTag = 'v3.2.0'
    ccloudDockerPullDeps = ['confluentinc/cc-base']
    maven_staging_url = "https://staging-ksqldb-maven.s3-us-west-2.amazonaws.com"
}

def defaultParams = [
    booleanParam(name: 'RELEASE_BUILD',
        description: 'Is this a release build. If so, GIT_REVISION must be specified, and only on-prem images will be built.',
        defaultValue: false),
    string(name: 'GIT_REVISION',
        description: 'The git SHA to build ksqlDB from.',
        defaultValue: ''),
    string(name: 'CCLOUD_GIT_REVISION',
        description: 'The cc-docker-ksql git SHA to build the CCloud ksqlDB image from.',
        defaultValue: ''),
    booleanParam(name: 'PROMOTE_TO_PRODUCTION',
        defaultValue: false,
        description: 'Promote images to production (DockerHub) for on-prem release build images only.'),
    string(name: 'KSQLDB_VERSION',
        defaultValue: '',
        description: 'KSQLDB version to promote to production.'),
    booleanParam(name: 'UPDATE_LATEST_TAG',
        defaultValue: false,
        description: 'Should the latest tag on docker hub be updated to point at this new image version. Only relevant if PROMOTE_TO_PRODUCTION is true.')
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

        // For a release build we remove the -SNAPSHOT from the version.
        config.ksql_db_version = config.ksql_db_version.tokenize("-")[0]
        config.docker_tag = config.ksql_db_version
    } else {
        config.docker_tag = config.ksql_db_version.tokenize("-")[0] + '-beta' + env.BUILD_NUMBER
        config.ccloud_docker_tag = 'v' + config.docker_tag
    }
    
    // Use revision param if provided, otherwise default to master
    config.revision = params.GIT_REVISION ?: 'refs/heads/master'
    config.ccloud_revision = params.CCLOUD_GIT_REVISION ?: 'refs/heads/vxia-update-common-version'

    // Configure the maven repo settings so we can download from the beta artifacts repo
    def settingsFile = "${env.WORKSPACE}/maven-settings.xml"
    def maven_packages_url = "${config.maven_packages_url}/${config.cp_version}/${config.packaging_build_number}/maven"
    def maven_staging_url = "${config.maven_staging_url}/maven"
    def settings = readFile('maven-settings-template.xml').replace('PACKAGES_MAVEN_URL', maven_packages_url).replace('STAGING_MAVEN_URL', maven_staging_url)
    writeFile file: settingsFile, text: settings
    mavenOptions = [artifactsPublisher(disabled: true),
        junitPublisher(disabled: true),
        openTasksPublisher(disabled: true)
    ]

    // TODO: add analogous section for ccloud promotion? maybe not necessary since requires manual spec update anyway
    if (params.PROMOTE_TO_PRODUCTION) {
        stage("Pulling Docker Images") {
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
        }

        stage("Tag and Push Images") {
            dockerHubCreds = usernamePassword(
                credentialsId: 'Jenkins Docker Hub Account',
                passwordVariable: 'DOCKER_PASSWORD',
                usernameVariable: 'DOCKER_USERNAME')
            withCredentials([dockerHubCreds]) {
                withDockerServer([uri: dockerHost()]) {
                    config.dockerRepos.each { dockerRepo ->
                        sh "docker login --username $DOCKER_USERNAME --password \'$DOCKER_PASSWORD\'"
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

    // Build on-prem ksqlDB image

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
                            writeFile file:'create-pip-conf-with-jfrog.sh', text:libraryResource('scripts/create-pip-conf-with-jfrog.sh')
                            writeFile file:'create-pypirc-with-jfrog.sh', text:libraryResource('scripts/create-pypirc-with-jfrog.sh')
                            writeFile file:'setup-credential-store.sh', text:libraryResource('scripts/setup-credential-store.sh')
                            writeFile file:'set-global-user.sh', text:libraryResource('scripts/set-global-user.sh')
                            sh '''
                                bash create-pip-conf-with-jfrog.sh
                                bash create-pypirc-with-jfrog.sh
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

                            if (!config.isPrJob) {
                                def git_tag = "v${config.docker_tag}-ksqldb"
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
        stage('Publish Maven Artifacts') {
            writeFile file: settingsFile, text: settings
            dir('ksql-db') {
                withCredentials([usernamePassword(credentialsId: 'JenkinsArtifactoryAccessToken', passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_USERNAME')]) {
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
                        // docker_tag for release builds does not include the build number
                        // here we add it back so an image version with the build number always exists
                        sh "docker tag ${config.dockerRegistry}${dockerRepo}:${config.docker_tag} ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-beta${env.BUILD_NUMBER}"
                        sh "docker push ${config.dockerRegistry}${dockerRepo}:${config.docker_tag}-beta${env.BUILD_NUMBER}"
                    }
                }
            }
        }
    }

    // Build CCloud ksqlDB image

    if (config.release) {
        // Do not build CCloud image in release mode
        return null
    }

    stage('Checkout CCloud ksqlDB Docker repo') {
        checkout changelog: false,
            poll: false,
            scm: [$class: 'GitSCM',
                branches: [[name: config.ccloud_revision]],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'ksqldb-ccloud-docker']],
                submoduleCfg: [],
                userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                    url: 'git@github.com:confluentinc/cc-docker-ksql.git']]
            ]
    }

    stage('Build CCloud KSQL Docker image') {
        dir('ksqldb-ccloud-docker') {
            archiveArtifacts artifacts: 'pom.xml'
            withCredentials([
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
                            writeFile file:'create-pip-conf-with-jfrog.sh', text:libraryResource('scripts/create-pip-conf-with-jfrog.sh')
                            writeFile file:'create-pypirc-with-jfrog.sh', text:libraryResource('scripts/create-pypirc-with-jfrog.sh')
                            writeFile file:'setup-credential-store.sh', text:libraryResource('scripts/setup-credential-store.sh')
                            writeFile file:'set-global-user.sh', text:libraryResource('scripts/set-global-user.sh')
                            sh '''
                                bash create-pip-conf-with-jfrog.sh
                                bash create-pypirc-with-jfrog.sh
                                bash setup-credential-store.sh
                                bash set-global-user.sh
                            '''

                            config.ccloudDockerPullDeps.each { dockerRepo ->
                                sh "docker pull ${config.dockerRegistry}${dockerRepo}:${config.ccloudDockerUpstreamTag}"
                            }

                            // Set the version of the parent project to use. TODO: not strictly necessary?
                            sh "mvn --batch-mode versions:update-parent -DparentVersion=\"[${config.cp_version}]\" -DgenerateBackupPoms=false"

                            if (!config.isPrJob) {
                                def git_tag = "v${config.docker_tag}-ksqldb"
                                sh "git add ."
                                sh "git commit -m \"build: Setting project version ${config.ksql_db_version} and parent version ${config.ksql_db_version}.\""
                                sh "git tag ${git_tag}"
                                sshagent (credentials: ['ConfluentJenkins Github SSH Key']) {
                                    sh "git push origin ${git_tag}"
                                }
                            }

                            cmd = "mvn --batch-mode -Pjenkins clean package dependency:analyze site validate -U "
                            cmd += "-DskipTests "
                            cmd += "-Dspotbugs.skip "
                            cmd += "-Dcheckstyle.skip "
                            cmd += "-Ddocker.tag=${config.ccloud_docker_tag} "
                            cmd += "-Ddocker.registry=${config.dockerRegistry} "
                            cmd += "-Ddocker.upstream-tag=${config.ccloudDockerUpstreamTag} "
                            cmd += "-Dskip.docker.build=false "
                            cmd += "-Dksql.version=${config.ksql_db_version} "

                            withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
                                sh cmd
                            }
                            step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])
                        }
                    }
                }
        }
    }

    // TODO: de-dup between on-prem and ccloud sections
    if(config.dockerScan){
        stage('Twistloc scan') {
            withDockerServer([uri: dockerHost()]) {
                dockerName = config.dockerRegistry+config.ccloudDockerRepo+":${config.ccloud_docker_tag}"
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

    if(config.dockerScan){
        stage('Twistloc publish') {
            withDockerServer([uri: dockerHost()]) {
                dockerName = config.dockerRegistry+config.ccloudDockerRepo+":${config.ccloud_docker_tag}"
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

    if (!config.isPrJob) {
        stage('Publish CCloud ksqlDB Docker Images') {
            withDockerServer([uri: dockerHost()]) {
                sh "docker tag ${config.dockerRegistry}${config.ccloudDockerRepo}:${config.ccloud_docker_tag} ${config.dockerRegistry}${config.ccloudDockerRepo}:master-latest"
                sh "docker push ${config.dockerRegistry}${config.ccloudDockerRepo}:${config.ccloud_docker_tag}"
                sh "docker push ${config.dockerRegistry}${config.ccloudDockerRepo}:master-latest"

                if (config.release) {
                    // docker_tag for release builds does not include the build number
                    // here we add it back so an image version with the build number always exists
                    sh "docker tag ${config.dockerRegistry}${config.ccloudDockerRepo}:${config.ccloud_docker_tag} ${config.dockerRegistry}${config.ccloudDockerRepo}:${config.docker_tag}-beta${env.BUILD_NUMBER}"
                    sh "docker push ${config.dockerRegistry}${config.ccloudDockerRepo}:${config.ccloud_docker_tag}-beta${env.BUILD_NUMBER}"
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
