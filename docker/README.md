# KSQL Demo Images

The included `Makefile` is a wrapper to `docker build`.

By default, local images and the public Docker registry are used. To work with a private Docker registry, set the `DOCKER_REGISTRY` environment variable when building images and launching the cluster.

To build SNAPSHOT images, configure `.m2/settings.xml` for SNAPSHOT dependencies. These must be available at build time.

```
# Set private registry, if needed. Trailing slash is required.
export DOCKER_REGISTRY=docker.example.com:8080/

make images  # Build images locally.
```
