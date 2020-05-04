# ksql-docker

Module for building ksqlDB docker images.

## To build locally

To build a new image with local changes:

1. Ensure you're logged in to docker:
    ```
    > docker login
    ```

1. Build docker images from local changes.
    ```
    > mvn -Pdocker package -DskipTests -Dspotbugs.skip -Dcheckstyle.skip  -Ddockerfile.skip=false -Dskip.docker.build=false -Ddocker.upstream-tag=latest-ubi8 -Ddocker.tag=local.build  -Ddocker.upstream-registry=''
    ```
   Change `docker.upstream-tag` if you want to depend on anything other than the latest master upstream, e.g. 5.4.x-latest.

1. Check the image was built:
    ```
    > docker image ls | grep local.build
    ```
    You should see the new image listed. For example:

    ```
    placeholder/confluentinc/ksqldb-docker       local.build   94210cd14384   About an hour ago   716MB
    ```
