# KSQL Demo Images

## Use Registry Images

1) In this directory, run:

```
docker-compose up -d
```

2) To launch KSQL CLI, run:

```
docker-compose exec ksql-cli \
	java -jar /usr/share/confluent/ksql-cli-1.0-SNAPSHOT-standalone.jar \
	local --bootstrap-server kafka:29092
```

## Local Images and Private Registry

The included `Makefile` includes shortcuts for building images, launching the cluster, and launching KSQL CLI. By default, local images and the public Docker repository are used.

To work with a private Docker registry, set the `DOCKER_REGISTRY` environment variable when building images and launching the cluster. You must log in to the private registry using `docker login`.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

```
# Set private registry, if needed. Trailing slash is required.
export DOCKER_REGISTRY=docker.example.com:8080/

# Log in to your private registry, if needed.
docker login docker.example.com:8080

make images  # Build images locally.

make compose  # Bring up the docker-compose cluster.

make client  # Launch KSQL client.
```
