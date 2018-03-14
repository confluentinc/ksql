# KSQL Demo Images

This project uses the `dockerfile-maven` plugin to build Docker images via Maven.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

Pushing images is currently handled via `docker push`, and is not part of the build.

```
mvn package -DskipTests  # Build local images

# Build images for a private registry; trailing '/' is required:
# mvn package -DskipTests -Ddocker.registry=docker.example.com:8080/ -Ddocker
.tag=$VERSION-$BUILD_NUMBER
```
