# ksql-docker

Module for building ksqlDB docker images.

## To build locally

To build a new image with local changes:

1. Ensure you're logged in to docker:
    ```shell
   $ docker login
    ```

2. Build docker images from local changes.
    ```shell
   $ mvn clean
   $ mvn -Pdocker package --settings maven-settings.xml -DskipTests -Dspotbugs.skip -Dcheckstyle.skip  -Ddockerfile.skip=false -Dskip.docker.build=false -Ddocker.upstream-tag=latest-ubi8 -Ddocker.tag=local.build  -Ddocker.upstream-registry=''
    ```
   Change `docker.upstream-tag` if you want to depend on anything other than the latest master upstream, e.g. 5.4.x-latest.

3. Check the image was built:
    ```shell
    $ docker image ls | grep local.build
    ```
    You should see the new image listed. For example:

    ```
    placeholder/confluentinc/ksqldb-docker       local.build   94210cd14384   About an hour ago   716MB
    ```
   
4. To use with the `docker-compose.yml` file in the root of the project, run the following additional commands to create
   copies of the image with the required `ksqldb-server` and `ksqldb-cli` image names:
   
   ```shell
   $ docker tag placeholder/confluentinc/ksqldb-docker:local.build placeholder/confluentinc/ksqldb-server:local.build
   $ docker tag placeholder/confluentinc/ksqldb-docker:local.build placeholder/confluentinc/ksqldb-cli:local.build
   ```
   
   To use the images uncomment the properties at the bottom of the `.env` file in the root of the project and run
   `docker-compose up -d` from the root directory.
