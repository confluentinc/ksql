ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base-new:${DOCKER_UPSTREAM_TAG}

ARG PROJECT_VERSION
ARG ARTIFACT_ID

USER root

# target directory must be one of the projects that ksql-run-class sets on the KSQL_CLASSPATH,
# of which the artifact ID (ksqldb-docker) is not one. thus the workaround of using ksqldb-rest-app
ADD --chown=appuser:appuser share/java/${ARTIFACT_ID}/* /usr/share/java/ksqldb-rest-app/
ADD --chown=appuser:appuser bin/* /usr/bin/
ADD --chown=appuser:appuser bin/docker/* /usr/bin/docker/
ADD --chown=appuser:appuser etc/* /etc/ksqldb/
#ADD --chown=appuser:appuser share/doc/* /usr/share/doc/${ARTIFACT_ID}/ # TODO: include docs

RUN echo "===> Installing confluent-hub..." \
    && wget http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz \
    && tar xf confluent-hub-client-latest.tar.gz \
    && rm confluent-hub-client-latest.tar.gz

RUN chmod +x /usr/bin/docker/configure
RUN chmod +x /usr/bin/docker/run

USER appuser

CMD ["/usr/bin/docker/run"]
