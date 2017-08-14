# KSQL Demo Images

This project uses the `dockerfile-maven` plugin to build Docker images via `mvn package`.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

```
mvn package -DskipTests  # Build local images

# Build images for a private registry; trailing '/' is required:
# mvn package -DskipTests -Ddocker.registry=docker.confluent.io:5000/
```
