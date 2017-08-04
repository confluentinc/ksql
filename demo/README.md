# KSQL Demo Images

## Run the images from Nexus

1) Configure your .m2/settings.xml for maven-snapshots (see new hire onboarding doc)

2) Log in to nexus: `docker login docker.confluent.io:5000`

3) In this directory, run `docker-compose up`

4) For a KSQL client, run `docker run --net=host --add-host=kafka:127.0.0.1 -it confluentinc/cp-ksql-cli`

## Build and run the images locally

To work with local images, see the included Makefile targets.
