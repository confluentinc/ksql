KSQL_VERSION ?= 1.0-SNAPSHOT

images:
ifndef DOCKER_REGISTRY
	@echo "***"
	@echo "*** DOCKER_REGISTRY is not set."
	@echo "*** Defaulting to public registry and local images. ***"
	@echo "***"
	@sleep 1
endif

	cd .. && mvn compile install -DskipTests

	cd ../ksql-examples && docker build . \
		--build-arg KSQL_VERSION=${KSQL_VERSION} \
		--build-arg DOCKER_REGISTRY=${DOCKER_REGISTRY} \
		-t ${DOCKER_REGISTRY}confluentinc/cp-ksql-examples

	cd ../ksql-cli && docker build . \
		--build-arg KSQL_VERSION=${KSQL_VERSION} \
		--build-arg DOCKER_REGISTRY=${DOCKER_REGISTRY} \
		-t ${DOCKER_REGISTRY}confluentinc/cp-ksql-cli
