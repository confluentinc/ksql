ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base:${DOCKER_UPSTREAM_TAG}

ARG PROJECT_VERSION
ARG ARTIFACT_ID

ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-standalone.jar /usr/share/java/${ARTIFACT_ID}/${ARTIFACT_ID}-${PROJECT_VERSION}-standalone.jar
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/etc/* /etc/ksql/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/
