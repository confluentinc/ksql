#!/usr/bin/env groovy

docker_oraclejdk8 {
    slackChannel = '#ksql'
    withPush = true
    upstreamProjects = 'confluentinc/schema-registry'
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    dockerRepos = ['confluentinc/ksql-examples', 'confluentinc/ksql-cli']
}
