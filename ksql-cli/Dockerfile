ARG DOCKER_REGISTRY

FROM ${DOCKER_REGISTRY}confluentinc/cp-base

ARG PROJECT_VERSION
ARG ARTIFACT_ID

ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-standalone.jar /usr/share/java/${ARTIFACT_ID}/${ARTIFACT_ID}-${PROJECT_VERSION}-standalone.jar
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/etc/* /etc/ksql/
ADD target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/
