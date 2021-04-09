FROM centos:centos8

ARG CP_VERSION

RUN mkdir ksql_test/

COPY * /ksql_test/

RUN yum install -y curl which java-1.8.0-openjdk-devel jq

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

RUN rpm --import https://packages.confluent.io/rpm/${CP_VERSION}/archive.key

RUN sed -i "s/\${CP_VERSION}/${CP_VERSION}/g" /ksql_test/confluent.repo

RUN cp /ksql_test/confluent.repo /etc/yum.repos.d/

RUN yum clean all && yum install -y confluent-kafka

RUN  yum install -y /ksql_test/*.rpm

RUN /ksql_test/test.sh /usr/bin /etc
