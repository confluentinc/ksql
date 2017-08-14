# KSQL Demo Images

This project uses the `dockerfile-maven` plugin to build Docker images as part of `mvn package`.

By default, local images and the public Docker registry are used. To work with a private Docker registry, set the `DOCKER_REGISTRY` environment variable when building images and launching the cluster.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

```
# Build images locally; trailing '/' is required:
mvn package -DskipTests -Ddocker.registry=docker.confluent.io:5000/
```
