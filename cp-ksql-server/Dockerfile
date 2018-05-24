ARG DOCKER_REGISTRY

FROM ${DOCKER_REGISTRY}confluentinc/cp-base

EXPOSE 8088

ENV COMPONENT=ksql-server
ENV KSQL_CLASSPATH=/usr/share/java/${COMPONENT}/*:/usr/share/java/monitoring-interceptors/*

ARG COMMIT_ID=unknown
LABEL io.confluent.docker.git.id=$COMMIT_ID
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker=true

ARG PROJECT_VERSION
ARG ARTIFACT_ID

RUN echo "===> Installing ControlCenter for monitoring interceptors ..."\
    && apt-get -qq update \
    && apt-get install -y confluent-control-center=${CONFLUENT_VERSION}${CONFLUENT_PLATFORM_LABEL}-${CONFLUENT_DEB_VERSION} \
    && echo "===> Cleaning up ..."  \
    && apt-get clean && rm -rf /tmp/* /var/lib/apt/lists/* 

ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/java/${ARTIFACT_ID}/* /usr/share/java/${COMPONENT}/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/etc/* /etc/ksql/

COPY include/etc/confluent/docker /etc/confluent/docker

RUN mkdir -p /etc/${COMPONENT} /etc/${COMPONENT}/secrets

RUN chmod +x /etc/confluent/docker/run
RUN chmod +x /etc/confluent/docker/configure
RUN chmod +x /etc/confluent/docker/launch
RUN chmod +x /etc/confluent/docker/ensure

CMD ["/etc/confluent/docker/run"]
