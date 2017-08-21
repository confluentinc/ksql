ARG DOCKER_REGISTRY

FROM ${DOCKER_REGISTRY}confluentinc/cp-base

ARG KSQL_VERSION
ARG ARTIFACT_ID

ADD target/${ARTIFACT_ID}-${KSQL_VERSION}-standalone.jar /usr/share/java/${ARTIFACT_ID}/${ARTIFACT_ID}-${KSQL_VERSION}-standalone.jar
ADD target/${ARTIFACT_ID}-${KSQL_VERSION}-package/bin/* /usr/bin/
ADD target/${ARTIFACT_ID}-${KSQL_VERSION}-package/etc/* /etc/ksql/
ADD target/${ARTIFACT_ID}-${KSQL_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/

RUN wget https://s3-us-west-2.amazonaws.com/grafana-releases/release/grafana_4.4.3_amd64.deb
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.2.deb

RUN dpkg -i grafana_4.4.3_amd64.deb
RUN dpkg -i elasticsearch-5.5.2.deb
