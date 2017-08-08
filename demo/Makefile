CONFLUENT_VERSION := 1.0-SNAPSHOT

all: images compose cli

images:
ifndef DOCKER_REGISTRY
	@echo "***"
	@echo "*** DOCKER_REGISTRY is not set."
	@echo "*** Defaulting to local and public images. ***"
	@echo "***"
	@sleep 1
endif

	cd .. && mvn compile install -DskipTests
	cd ../ksql-examples && docker build . --build-arg DOCKER_REGISTRY=${DOCKER_REGISTRY} -t ${DOCKER_REGISTRY}confluentinc/cp-ksql-examples
	cd ../ksql-cli && docker build . --build-arg DOCKER_REGISTRY=${DOCKER_REGISTRY} -t ${DOCKER_REGISTRY}confluentinc/cp-ksql-cli

compose:
	docker-compose -f docker-compose.yml up -d

cli:
	docker-compose exec ksql-cli java -jar /usr/share/confluent/ksql-cli-${CONFLUENT_VERSION}-standalone.jar local --bootstrap-server kafka:29092
