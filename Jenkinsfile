// implement a release build process

def config = jobConfig {
    owner: ksql
    slackChannel: 'ksql-notifications'
}

//should i move this to config?
def ksql-db-version = "0.6.0"
def cp_version = "v5.5.0-beta......."

def job = {

    // get the root folder of the job, not the folder of the repo that was checked out by default

    stage('Checkout KSQL') {
        checkout changelog: false,
            poll: false,
            scm: [$class: 'GitSCM',
                branches: [[name: 'refs/heads/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [],
                submoduleCfg: [],
                relativeTargetDir: 'ksql-db'
                userRemoteConfigs: [[credentialsId: 'ConfluentJenkins Github SSH Key',
                    url: 'git@github.com:confluentinc/ksql.git']]]
    }

    stage('Set Project Versions') {
        dir('ksql-db') {
            // set the project versions in the pom files
        }
    }

    stage('Set Dependnecy Versions') {
        dir('ksql-db') {
            // set confluent dependencies to beta version, make sure pom file is hard coded to this version
        }
    }

    stage('Tag Repo') {
        dir('ksql-db') {
            // commit the changes to the pom files and tag, and push the tag
        }
    }

    stage('Build KSQL-DB') {
        dir('ksql-db') {

        }
    }
}

def post = {}

runJob config, job, post


