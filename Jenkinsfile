#!/usr/bin/env groovy
withDockerServer([uri: 'tcp://172.31.41.2:2375/']) {
    common {
        nodeLabel = 'docker-oraclejdk8'
        slackChannel = '#ksql'
    }
}
