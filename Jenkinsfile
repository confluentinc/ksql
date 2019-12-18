// ---------CONFIGURATION----------

def config = {
  // change these when you want to release a new image
  ksql_db_version         = '0.6.0'
  cp_version              = "5.5.0-beta191113214629"
  cp_package_build_number = '1'

  // Jenkins
  owner         = 'ksql'
  cron          = '@daily'
  nodeLabel     = 'docker-oraclejdk8-compose-swarm'
  slackChannel  = '#ksql-alerts'

  // internal constants
  modules       = ['confluentinc/ksql-cli', 'confluentinc/ksql-rest-app']
  docker_repos  = ['confluentinc/ksqldb-cli', 'confluentinc/ksqldb-server']

  // external constants - these should not change in normal operation
  maven_packages_url  = "https://jenkins-confluent-packages-beta-maven.s3-us-west-2.amazonaws.com"
  ecr_registry        = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
}

def defaultParams = [
  string(
    name: 'GIT_REVISION',
    description: 'The Git SHA to use for this build',
    defaultValue: 'refs/heads/master'
  ),
  booleanParam(
    name: 'RELEASE',
    description: 'Create a GitHub tag and unique docker image for GIT_REVISION',
    defaultValue: false
  ),
  booleanParam(
    name: 'PROMOTE_TO_PRODUCTION',
    description: 'Publishes the images for the RELEASE_CANDIDATE_TAG to the public DockerHub repository under the PRODUCTION_TAG',
    defaultValue: false
  ),
  stringParam(
    name: 'RELEASE_CANDIDATE_TAG',
    description: 'The docker tag in ECR to promote to production (e.g. `0.6.0`)',
    defaultValue: ''
  ),
  stringParam(
    name: 'PRODUCTION_TAG',
    description: 'The tag that will be published to the public DockerHub repository',
    defaultValue: ''
  )
]

def mavenOptions = [
  artifactsPublisher(disabled: true),
  junitPublisher(disabled: true),
  openTasksPublisher(disabled: true)
]

def jenkinsNexus        = usernamePassword(credentialsId: 'Jenkins Nexus Account',          passwordVariable: 'NEXUS_PASSWORD',       usernameVariable: 'NEXUS_USERNAME')
def jenkinsArtifactory  = usernamePassword(credentialsId: 'JenkinsArtifactoryAccessToken',  passwordVariable: 'ARTIFACTORY_PASSWORD', usernameVariable: 'ARTIFACTORY_USERNAME')
def jenkinsDockerHub    = usernamePassword(credentialsId: 'Jenkins Docker Hub Account',     passwordVariable: 'DOCKER_PASSWORD',      usernameVariable: 'DOCKER_USERNAME')
def jenkinsGitHub       = usernameColonPassword(credentialsId: 'Jenkins GitHub Account',    variable: 'GIT_CREDENTIAL')

// --------HELPER CLOSURES---------

def validateConfig = {
  stage('Validate Configs') {
    if (params.RELEASE && params.GIT_REVISION == 'ref/heads/master') {
      currentBuild.result = 'ABORTED';
      error('RELEASE requires specification of a GIT_REVISION')
    }

    if (params.PROMOTE_TO_PRODUCTION && (params.RELEASE_CANDIDATE_TAG == '' || params.PRODUCTION_TAG == '')) {
      currentBuild.result = 'ABORTED';
      error('PROMOTE_TO_PRODUCTION requires both RELEASE_CANDIDATE_TAG and PRODUCTION_TAG')
    }
  }
}

def configureMavenBeta = {
  stage('Configure Maven Repo') {
    maven_packages_url = "${config.maven_packages_url}/${config.cp_version}/${config.cp_package_build_number}"
    settings = readFile('maven-settings-template.xml').replace('PACKAGES_MAVEN_URL', config.maven_packages_url)
    writeFile file: config.mvn_settings_file, text: settings
  }
}

def configureArtifactory = {
  stage('Configure Artifactory') {
    withCredentials([jenkinsNexus, jenkinsArtifactory]) {
      sh 'set +x'
      sh 'echo $ARTIFACTORY_PASSWORD | docker login confluent-docker.jfrog.io -u $ARTIFACTORY_USERNAME --password-stdin'

      writeFile file:'create-pip-conf-with-nexus.sh', text:libraryResource('scripts/create-pip-conf-with-nexus.sh')
      writeFile file:'create-pypirc-with-nexus.sh', text:libraryResource('scripts/create-pypirc-with-nexus.sh')
      sh 'bash create-pip-conf-with-nexus.sh'
      sh 'bash create-pypirc-with-nexus.sh'
      sh 'set -x'
    }
  }
}

def setupGitHubCredentials = {
  stage('Set Up GitHub Credentials') {
    withCredentials([jenkinsGitHub]) {
      writeFile file:'setup-credential-store.sh', text:libraryResource('scripts/setup-credential-store.sh')
      writeFile file:'set-global-user.sh', text:libraryResource('scripts/set-global-user.sh')

      sh 'set +x'
      sh 'bash setup-credential-store.sh'
      sh 'bash set-global-user.sh'
      sh 'set -x'
    }
  }
}

/**
 * Extracts the necessary ECR credentials and pulls the specified docker images. This must
 * be run within a withDockerServer block
 *
 * @param registry  the registry to use
 * @param repo      the repository to use
 * @param tag       the tag to use
 */
def dockerPull = {registry, repo, tag ->
  // this script is in https://github.com/confluentinc/jenkins-common (note: not OSS!)
  writeFile file:'extract-iam-credential.sh', text:libraryResource('scripts/extract-iam-credential.sh')

  sh '''
      # make sure not to print any credentials to stdout
      set +x
      bash extract-iam-credential.sh
      LOGIN_CMD=$(aws ecr get-login --no-include-email --region us-west-2)
      $LOGIN_CMD

      set -x
  '''

  sh "docker pull ${registry}${repo}:${tag}"
}

// --------PIPELINE TASKS----------

def updateConfig = {c -> c.properties = [parameters(defaultParams)]}
def finalConfig = jobConfig(config, [:], updateConfig)

def job = {
  validateConfig(config, params)

  config.revision           = params.GIT_REVISION
  config.release            = params.RELEASE
  config.docker_tag         = params.RELEASE ? 'master-latest' : config.ksql_db_version + '-' + env.BUILD_NUMBER
  config.ksql_db_version    = config.ksql_db_version + (params.RELEASE ? '' : '-SNAPSHOT')

  config.mvn_settings_file  = "${env.WORKSPACE}/maven-settings.xml"
  configureMavenBeta()

  stage('Checkout ksqlDB') {
      checkout changelog: false,
          poll: false,
          scm: [$class: 'GitSCM',
              branches: [[name: config.revision]],
              doGenerateSubmoduleConfigurations: false,
              extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'ksql-db']],
              submoduleCfg: [],
              userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key', url: 'git@github.com:confluentinc/ksql.git']]
          ]
  }

  if (params.PROMOTE_TO_PRODUCTION) {
    stage('Promote to Production') {
      withCredentials([jenkinsArtifactory, jenkinsDockerHub]) {
        withDockerServer([uri: dockerHost()]) {
          config.docker_repos.each { repo ->
            dockerPull(config.ecr_registry, repo, params.RELEASE_CANDIDATE_TAG)

            source  = "${config.ecr_registry}${repo}:${params.RELEASE_CANDIDATE_TAG}"
            dest    = "${repo}:${params.PRODUCTION_TAG}"

            sh """
                set +x
                docker login --username $DOCKER_USERNAME --password \'$DOCKER_PASSWORD\'
                set -x

                docker tag ${source} ${dest}
                docker push ${dest}
                docker push ${repo}:latest
            """
          }
        }
      }
    }

    return null
  }

  stage('Build ksqlDB') {
    dir ('ksql-db') {
      archiveArtifacts artifacts: 'pom.xml'

      withDockerServer([uri: dockerHost()]) {
        withMaven(globalMavenSettingsFilePath: config.mvn_settings_file, options: mavenOptions) {
          configureArtifactory()

          // Step 1: change the ksqlDB maven poms to use the version specified
          // in the config instead of whatever is in git (likely the CP version)
          sh "mvn --batch-mode versions:set -DnewVersion=${config.ksql_db_version} -DgenerateBackupPoms=false"

          // Step 2: change the CP version to the beta version that we want to
          // use in the ksqlDB build (essentially pinning it)
          sh "mvn --batch-mode versions:update-parent -DparentVersion=\"[${config.cp_version}]\" -DgenerateBackupPoms=false"

          // Step 3: pull any required dependencies to build the docker image
          dockerPull(config.ecr_registry, "confluentinc/cp-base-new", "${config.cp_version}-latest")

          // Step 4: build the artifacts and the docker images
          cmd  = "mvn --batch-mode -Pjenkins clean package dependency:analyze site validate -U "
          cmd += "-DskipTests "
          cmd += "-Dspotbugs.skip "
          cmd += "-Dcheckstyle.skip "
          cmd += "-Ddocker.tag=${config.docker_tag} "
          cmd += "-Ddocker.registry=${config.ecr_registry} "
          cmd += "-Ddocker.upstream-tag=${config.cp_version}-latest "
          cmd += "-Dskip.docker.build=false "

          withEnv(['MAVEN_OPTS=-XX:MaxPermSize=123M']) {sh cmd}

          // Step 5: rename the docker images from the maven name (e.g. ksql-rest-app)
          // to the desired image name (e.g. ksqldb-server)
          config.modules.eachWithIndex { module, index ->
            image   = config.docker_repos[index]
            source  = "${config.ecr_registry}${module}:${config.docker_tag}"
            dest    = "${config.ecr_registry}${image}:${config.docker_tag}"

            sh "docker tag ${source} ${dest}"
          }

          // Step 6: in the case of a release, tag the ksql git repo to
          // have repeatable builds
          if (params.RELEASE) {
            setupGitHubCredentials()
            def git_tag = "v${config.ksql_db_version}-ksqldb"
            sshagent (credentials: ['ConfluentJenkins Github SSH Key']) {
              sh """
                  git add .
                  git commit -m \"build: setting project version ${config.ksql_db_version} and parent version ${config.cp_version}\"
                  git tag ${git_tag}
                  git push origin ${git_tag}
              """
            }
          }
        }
      }
    }
  }

  stage('Twistloc') {
    withDockerServer([uri: dockerHost()]) {
      config.docker_repos.each { repo ->
        image = "${config.ecr_registry}${repo}:${config.docker_tag}"

        echo "Twistloc Scan: ${image}"
        twistlockScan    ca: '',
                         cert: '',
                         compliancePolicy: 'critical',
                         dockerAddress: dockerHost(),
                         gracePeriodDays: 0,
                         ignoreImageBuildTime: true,
                         image: image,
                         key: '',
                         logLevel: 'true',
                         policy: 'warn',
                         requirePackageUpdate: false,
                         timeout: 10

        echo "Twistloc Publish: ${image}"
        twistlockPublish ca: '',
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

  stage('Publish Docker Images (ECR)') {
    withDockerServer([uri: dockerHost()]) {
      config.docker_repos.each { repo ->
        source  = "${config.ecr_registry}${repo}:${config.docker_tag}"
        sh "docker push ${source}"

        // in the case of a release, we want to also update master-latest
        // alongside the usual push
        if (config.release) {
          dest = "${config.ecr_registry}${repo}:master-latest"
          sh "docker tag ${source} ${dest}"
          sh "docker push ${dest}"
        }
      }
    }
  }
}

def post = {
  withDockerServer([uri: dockerHost()]) {
    repos = config.modules + config.docker_repos
    registries = params.PROMOTE_TO_PRODUCTION ? ['', config.ecr_registry] : [config.ecr_registry]

    registries.each { registry ->
      repos.reverse().each { repo ->
        target = "${registry}${repo}"
        sh """
            images=\$(docker images -q ${target})
            if [[ ! -z \$images ]]; then
                docker rmi -f \$images || true
            else
                echo 'No images for ${target} need cleanup'
            fi
        """
      }
    }
  }

  commonPost(finalConfig)
}

runJob finalConfig, job, post

