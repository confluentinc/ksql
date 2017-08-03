all: build compose cli

build:
	cd .. && mvn compile install -DskipTests
	cd ../ksql-examples && docker build . -t confluentinc/cp-ksql-examples
	cd ../ksql-cli && docker build . -t confluentinc/cp-ksql-cli

compose:
	docker-compose up -d

cli:
	docker run --net=host --add-host=kafka:127.0.0.1 -it confluentinc/cp-ksql-cli
