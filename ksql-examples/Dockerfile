ARG DOCKER_REGISTRY

FROM "${DOCKER_REGISTRY}confluentinc/cp-base"

ARG KSQL_VERSION

ADD target/ksql-examples-${KSQL_VERSION}-standalone.jar /usr/share/confluent/ksql-examples-${KSQL_VERSION}-standalone.jar
