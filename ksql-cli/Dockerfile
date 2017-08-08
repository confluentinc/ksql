ARG DOCKER_REGISTRY

FROM "${DOCKER_REGISTRY}confluentinc/cp-base"

ARG KSQL_VERSION

ADD "target/ksql-cli-${KSQL_VERSION}-standalone.jar" "/usr/share/confluent/ksql-cli-${KSQL_VERSION}-standalone.jar"
