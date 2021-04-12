FROM ubuntu:bionic

ARG CP_VERSION

RUN mkdir ksql_test/

COPY * /ksql_test/

RUN apt-get update && apt-get install -y wget gnupg software-properties-common openjdk-8-jdk curl jq

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN wget -qO - https://packages.confluent.io/deb/${CP_VERSION}/archive.key | apt-key add -

RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/${CP_VERSION} stable main"

RUN apt-get update && apt-get install -y confluent-kafka

RUN mkdir /ksql_test/confluent_platform && tar -zxf /ksql_test/*.tar.gz -C /ksql_test/confluent_platform --strip-components 1

RUN /ksql_test/test.sh /ksql_test/confluent_platform/bin /ksql_test/confluent_platform/etc
