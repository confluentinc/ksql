ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base:${DOCKER_UPSTREAM_TAG}

ENV COMPONENT=ksql-cli
ENV KSQL_CLASSPATH=/usr/share/java/${COMPONENT}/*

ARG COMMIT_ID=unknown
LABEL io.confluent.docker.git.id=$COMMIT_ID
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker=true

ARG PROJECT_VERSION
ARG ARTIFACT_ID

ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/java/${ARTIFACT_ID}/* /usr/share/java/${COMPONENT}/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/etc/* /etc/ksql/

COPY include/etc/confluent/docker /etc/confluent/docker

RUN mkdir -p /etc/${COMPONENT} /etc/${COMPONENT}/secrets /var/log/${COMPONENT}

RUN bash /etc/confluent/docker/configure
ENV KSQL_LOG4J_OPTS=-Dlog4j.configuration=file:/etc/${COMPONENT}/log4j.properties

ENTRYPOINT ["ksql"]
CMD ["http://localhost:8088"]

