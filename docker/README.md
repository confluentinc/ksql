# KSQL Demo Images

This project uses the `dockerfile-maven` plugin to build Docker images via Maven.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

Pushing images is currently handled via `docker push`, and is not part of the build.

```
mvn compile install -DskipTests  # Build local images

# Build images for a private registry; trailing '/' is required:
# mvn compile install -DskipTests -Ddocker.registry=docker.example.com:8080/ -Ddocker.tag=0.1-SNAPSHOT-$BUILD_NUMBER
```
