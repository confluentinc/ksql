FROM ubuntu:bionic

ARG CP_VERSION

RUN mkdir ksql_test/ && cd ksql_test

COPY * /ksql_test/

RUN apt-get update && apt-get install -y wget gnupg software-properties-common openjdk-8-jdk curl jq

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN wget -qO - https://packages.confluent.io/deb/${CP_VERSION}/archive.key | apt-key add -

RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/${CP_VERSION} stable main"

RUN apt-get update && apt-get install -y confluent-kafka

RUN apt-get install -y /ksql_test/*.deb

RUN /ksql_test/test.sh
