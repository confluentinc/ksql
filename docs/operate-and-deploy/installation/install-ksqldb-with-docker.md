---
layout: page
title: Configure ksqlDB with Docker
tagline: Configure ksqlDB by using Docker containers
description: Learn how to install ksqlDB in various configurations by using Docker containers
keywords: ksqldb, docker, container, install, configure, production
---

You can deploy ksqlDB by using Docker containers. Confluent maintains images at
[Docker Hub](https://hub.docker.com/u/confluentinc) for
[ksqlDB Server](https://hub.docker.com/r/confluentinc/ksqldb-server/) and the
[ksqlDB command-line interface (CLI)](https://hub.docker.com/r/confluentinc/ksqldb-cli/).

Use the following settings to start containers that run ksqlDB in various
configurations.

Assign configuration settings in the Docker run command
-------------------------------------------------------

You can pass configuration settings dynamically into containers by using
environment variables. When you start a container, set up the
configuration with the `-e` or `--env` flags in the `docker run`
command.

For a complete list of ksqlDB parameters, see
[ksqlDB Configuration Parameter Reference](/reference/server-configuration).

In most cases, to assign a ksqlDB configuration parameter in a container,
you prepend the parameter name with `KSQL_` and substitute the
underscore character for periods. For example, to assign the
`ksql.queries.file` setting in your `docker run` command, specify:

```
-e KSQL_KSQL_QUERIES_FILE=<path-in-container-to-sql-file>
```

Also, you can set configuration options by using the `KSQL_OPTS`
environment variable. For example, to assign the `ksql.queries.file`
setting in your `docker run` command, specify:

```
-e KSQL_OPTS="-Dksql.queries.file=/path/in/container/queries.sql"
```

Properties set with `KSQL_OPTS` take precedence over values specified in
the ksqlDB configuration file. For more information, see
[Setting ksqlDB Server Parameters](server-config/index.md#setting-ksqldb-server-parameters).

ksqlDB Server configurations
----------------------------

The following commands show how to run ksqlDB Server in different
configurations.

### ksqlDB Interactive Server Settings

Develop your ksqlDB applications by using the ksqlDB command-line interface
(CLI), or the graphical interface in {{ site.c3 }}, or both together.

Run a ksqlDB Server that enables manual interaction by using the ksqlDB CLI:

```bash
docker run -d \
  -p 127.0.0.1:8088:8088 \
  -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KSQL_LISTENERS=http://0.0.0.0:8088/ \
  -e KSQL_KSQL_SERVICE_ID=ksql_service_2_ \
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

`KSQL_BOOTSTRAP_SERVERS`

:   A list of hosts for establishing the initial connection to the Kafka
    cluster.

`KSQL_KSQL_SERVICE_ID`

:   The service ID of the ksqlDB Server, which is used as the prefix for
    the internal topics created by ksqlDB.

`KSQL_LISTENERS`

:   A list of URIs, including the protocol, that the broker listens on.
    If you are using IPv6, set it to `http://[::]:8088`.

`KSQL_CLASSPATH`

:   The classpath to use for the server, which can contain any additional
    JARs that you may need to run your plugins and UDFs.

In interactive mode, a ksqlDB CLI instance running outside of Docker can
connect to the ksqlDB server running in Docker.

### ksqlDB Headless Server Settings

You can deploy ksqlDB Server in a non-interactive, or
*headless*, mode. In headless mode, interactive use of the ksqlDB cluster
is disabled, and you configure ksqlDB Server with a predefined `.sql` file
and the `KSQL_KSQL_QUERIES_FILE` setting. For more information, see
[Non-interactive (Headless) ksqlDB Usage](server-config/index.md#non-interactive-headless-ksqldb-usage).

Use the following command to run a headless, standalone ksqlDB Server
instance in a container:

```bash
docker run -d \
  -v /path/on/host:/path/in/container/ \
  -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KSQL_KSQL_SERVICE_ID=ksql_standalone_1_ \
  -e KSQL_KSQL_QUERIES_FILE=/path/in/container/queries.sql \
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

`KSQL_BOOTSTRAP_SERVERS`

:   A list of hosts for establishing the initial connection to the Kafka
    cluster.

`KSQL_KSQL_SERVICE_ID`

:   The service ID of the ksqlDB server, which is used as the prefix for
    the internal topics created by ksqlDB.

`KSQL_KSQL_QUERIES_FILE`

:   A file that specifies predefined SQL queries.

`KSQL_CLASSPATH`

:   The classpath to use for the server, which can contain any additional
    JARs that you may need to run your plugins and UDFs.

### Connect ksqlDB Server to a secure Kafka Cluster, like Confluent Cloud

ksqlDB Server runs outside of your {{ site.ak }} clusters, so you need to
specify in the container environment how ksqlDB Server connects with a
{{ site.ak }} cluster.

Run a ksqlDB Server that uses a secure connection to a {{ site.ak }} cluster:

```bash
docker run -d \
  -p 127.0.0.1:8088:8088 \
  -e KSQL_BOOTSTRAP_SERVERS=REMOTE_SERVER1:9092,REMOTE_SERVER2:9093,REMOTE_SERVER3:9094 \
  -e KSQL_LISTENERS=http://0.0.0.0:8088/ \
  -e KSQL_KSQL_SERVICE_ID=default_ \
  -e KSQL_KSQL_SINK_REPLICAS=3 \
  -e KSQL_KSQL_STREAMS_REPLICATION_FACTOR=3 \
  -e KSQL_KSQL_INTERNAL_TOPIC_REPLICAS=3 \
  -e KSQL_SECURITY_PROTOCOL=SASL_SSL \
  -e KSQL_SASL_MECHANISM=PLAIN \
  -e KSQL_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<username>\" password=\"<strong-password>\";" \
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

`KSQL_BOOTSTRAP_SERVERS`

:   A list of hosts for establishing the initial connection to the Kafka
    cluster.

`KSQL_KSQL_SERVICE_ID`

:   The service ID of the ksqlDB Server, which is used as the prefix for
    the internal topics created by ksqlDB.

`KSQL_LISTENERS`

:   A list of URIs, including the protocol, that the broker listens on.
    If you are using IPv6, set it to `http://[::]:8088`.

`KSQL_KSQL_SINK_REPLICAS`

:   The default number of replicas for the topics created by ksqlDB. The
    default is one.

`KSQL_KSQL_STREAMS_REPLICATION_FACTOR`

:   The replication factor for internal topics, the command topic, and
    output topics.

`KSQL_KSQL_INTERNAL_TOPIC_REPLICAS`

:   The number of replicas for the internal topics created by ksqlDB Server.
    The default is 1.

`KSQL_SECURITY_PROTOCOL`

:   The protocol that your Kafka cluster uses for security.

`KSQL_SASL_MECHANISM`

:   The SASL mechanism that your Kafka cluster uses for security.

`KSQL_SASL_JAAS_CONFIG`

:   The Java Authentication and Authorization Service (JAAS)
    configuration.

Learn how to [Configure Security for ksqlDB](server-config/security.md).

### Configure a ksqlDB Server by Using Java System Properties

Use the `KSQL_OPTS` environment variable to assign configuration
settings by using Java system properties. Prepend the ksqlDB setting name
with `-D`. For example, to set the ksqlDB service identifier in the
`docker run` command, use:

```
-e KSQL_OPTS="-Dksql.service.id=<your-service-id>"
```

Run a ksqlDB Server with a configuration that's defined by Java
properties:

```bash
docker run -d \
  -v /path/on/host:/path/in/container/ \
  -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KSQL_OPTS="-Dksql.service.id=ksql_service_3_  -Dksql.queries.file=/path/in/container/queries.sql" \
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

`KSQL_BOOTSTRAP_SERVERS`

:   A list of hosts for establishing the initial connection to the Kafka
    cluster.

`KSQL_OPTS`

:   A space-separated list of Java options.

The previous example assigns two settings, `ksql.service.id` and
`ksql.queries.file`. Specify more configuration settings by adding them
in the `KSQL_OPTS` line. Remember to prepend each setting name with
`-D`.

### View ksqlDB Server Logs

Use the `docker logs` command to view ksqlDB logs that are generated from
within the container:

```bash
docker logs -f <container-id>
```

Your output should resemble:

```
[2019-01-16 23:43:05,591] INFO stream-thread [_confluent-ksql-default_transient_1507119262168861890_1527205385485-71c8a94c-abe9-45ba-91f5-69a762ec5c1d-StreamThread-17] Starting (org.apache.kafka.streams.processor.internals.StreamThread:713)
...
```

### Enable the ksqlDB Processing Log

ksqlDB emits a log of record processing events, called the processing log,
to help you debug SQL queries. For more information, see
[ksqlDB Processing Log](../../reference/processing-log.md).

Assign the following configuration settings to enable the processing
log.

```
# — Processing log config —
KSQL_LOG4J_PROCESSING_LOG_BROKERLIST: kafka:29092
KSQL_LOG4J_PROCESSING_LOG_TOPIC: demo_processing_log
KSQL_KSQL_LOGGING_PROCESSING_TOPIC_NAME: demo_processing_log
KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
```

### Mount Volumes

Various features (plugins, UDFs, embedded connectors) may require that you mount
volumes to the docker image. To do this, follow [the official docker documentation](https://docs.docker.com/storage/volumes/).

As an example using `docker-compose`, you can mount a udf directory and use it like this:

```yaml
  ksqldb-server:
    image: confluentinc/ksqldb-server:{{ site.ksqldbversion }}
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./extensions/:/opt/ksqldb-udfs"
    environment:
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      # Configuration for UDFs
      KSQL_KSQL_EXTENSION_DIR: "/opt/ksqldb-udfs"
```

ksqlDB Command-line Interface (CLI)
-----------------------------------

Develop the SQL queries and statements for your real-time streaming
applications by using the ksqlDB CLI, or the graphical interface in
{{ site.c3 }}, or both together. The ksqlDB CLI connects to a running ksqlDB
Server instance to enable inspecting Kafka topics and creating ksqlDB
streams and tables. For more information, see
[Configure ksqlDB CLI](cli-config.md).

The following commands show how to run the ksqlDB CLI in a container and
connect to a ksqlDB Server.

### Connect ksqlDB CLI to a Dockerized ksqlDB Server

Run a ksqlDB CLI instance in a container and connect to a ksqlDB Server
that's running in a different container.

```bash
# Run ksqlDB Server.
docker run -d -p 10.0.0.11:8088:8088 \
  -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KSQL_OPTS="-Dksql.service.id=ksql_service_3_  -Dlisteners=http://0.0.0.0:8088/" \  
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}

# Connect the ksqlDB CLI to the server.
docker run -it confluentinc/ksqldb-cli ksql http://10.0.0.11:8088 
```

`KSQL_BOOTSTRAP_SERVERS`

:   A list of hosts for establishing the initial connection to the Kafka
    cluster.

`KSQL_OPTS`

:   A space-separated list of Java options. If you are using IPv6, set
    `listeners` to `http://[::]:8088`.

The Docker network created by ksqlDB Server enables you to connect with a
dockerized ksqlDB CLI.

### Start ksqlDB CLI With a Provided Configuration File

Set up a ksqlDB CLI instance by using a configuration file, and run it
in a container:

```bash
# Assume ksqlDB Server is running.
# Ensure that the configuration file exists.
ls /path/on/host/ksql-cli.properties

docker run -it \
  -v /path/on/host/:/path/in/container  \
  confluentinc/ksqldb-cli:{{ site.ksqldbversion }} ksql http://10.0.0.11:8088 \
  --config-file /path/in/container/ksql-cli.properties
```

### Connect ksqlDB CLI to a ksqlDB Server Running on Another Host (Cloud)

Run a ksqlDB CLI instance in a container and connect to a remote ksqlDB
Server host:

```bash
docker run -it confluentinc/ksqldb-cli:{{ site.ksqldbversion }} ksql \
  http://ec2-blah.us-blah.compute.amazonaws.com:8080
```

Your output should resemble:

```
... 
Copyright 2017-2020 Confluent Inc.

CLI v{{ site.ksqldbversion }}, Server v{{ site.ksqldbversion }} located at http://ec2-blah.us-blah.compute.amazonaws.com:8080

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

Interact with ksqlDB running in a Docker container
--------------------------------------------------

You can communicate with ksqlDB Server and the ksqlDB CLI when they run in
Docker containers. The following examples show common tasks with ksqlDB
processes that run in containers.

-   [Wait for an HTTP Endpoint to Be Available](#wait-for-an-http-endpoint-to-be-available)
-   [Wait for a Particular Phrase in a Container's Log](#wait-for-a-particular-phrase-in-a-containers-log)
-   [Run Custom Code Before Launching a Container's Program](#run-custom-code-before-launching-a-containers-program)
-   [Execute a ksqlDB script in the ksqlDB CLI](#execute-a-ksqldb-script-in-the-ksqldb-cli)

### Wait for an HTTP endpoint to be available

Sometimes, a container reports its state as `up` before it's actually
running. In this case, the docker-compose `depends_on` dependencies
aren't sufficient. For a service that exposes an HTTP endpoint, like
ksqlDB Server, you can force a script to wait before running a client that
requires the service to be ready and available.

Use the following bash commands to wait for ksqlDB Server to be available:

```bash
echo -e "\n\n⏳ Waiting for ksqlDB to be available before launching CLI\n"
while [ $(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip-address>:8088/) -eq 000 ]
do 
  echo -e $(date) "ksqlDB Server HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip-address>:8088/) " (waiting for 200)"
  sleep 5
done
```

This script pings the ksqlDB Server at `<ksql-server-ip-address>:8088`
every five seconds, until it receives an HTTP 200 response.

!!! note
      The previous script doesn't work with "headless" deployments of ksqlDB
      Server, because headless deployments don't have a REST API server.

To launch the ksqlDB CLI in a container only after ksqlDB Server is
available, use the following Docker Compose command:

```bash
docker-compose exec ksql-cli bash -c \
'echo -e "\n\n⏳ Waiting for ksqlDB to be available before launching CLI\n"; while [ $(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip-address>:8088/) -eq 000 ] ; do echo -e $(date) "ksqlDB Server HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip-address>:8088/) " (waiting for 200)" ; sleep 5 ; done; ksql http://<ksql-server-ip-address>:8088'
```

### Wait for a particular phrase in a container's log

Use the `grep` command and
[bash process substitution](http://tldp.org/LDP/abs/html/process-sub.html)
to wait until the a specific phrase occurs in the Docker Compose log:

```bash
export CONNECT_HOST=<container-name>
echo -e "\n--\n\nWaiting for Kafka Connect to start on $CONNECT_HOST … ⏳"
grep -q "Kafka Connect started" <(docker-compose logs -f $CONNECT_HOST)
```

### Run custom code before launching a container's program

You can run custom code, like downloading a dependency or moving a file,
before a ksqlDB process starts in a container. Use Docker Compose to
overlay a change on an existing image.

#### Get the container's default command

Discover the default command that the container runs when it launches,
which is either `Entrypoint` or `Cmd`:

```bash
docker inspect --format={% raw %}'{{.Config.Entrypoint}}'{% endraw %} confluentinc/ksqldb-server:{{ site.ksqldbversion }}
docker inspect --format={% raw %}'{{.Config.Cmd}}'{% endraw %} confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

Your output should resemble:

```
    []
    [/usr/bin/docker/run]
```

In this example, the default command is `/usr/bin/docker/run`.

#### Run custom commands before the ksqlDB process starts

In a Docker Compose file, add the commands that you want to run before
the main process starts. Use the `command` option to override the
default command. In the following example, the `command` option creates
a directory and downloads a tar archive into it.

```yaml
ksql-server:
  image: confluentinc/ksqldb-server:{{ site.ksqldbversion }}
  depends_on:
    - kafka
  environment:
    KSQL_BOOTSTRAP_SERVERS: <bootstrap-server-ip>:29092
    KSQL_LISTENERS: http://0.0.0.0:8088
  command: 
    - /bin/bash
    - -c 
    - |
      mkdir -p /data/maxmind
      cd /data/maxmind
      curl https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz | tar xz 
      /usr/bin/docker/run
```

After the `mkdir`, `cd`, `curl`, and `tar` commands run, the
`/usr/bin/docker/run` command starts the `ksqldb-server` image
with the specified settings.

!!! note
      The literal block scalar, `- |`, enables passing multiple arguments to
      `command`, by indicating that the following lines are all part of the
      same entry.

### Execute a ksqlDB script in the ksqlDB CLI

The following Docker Compose YAML runs ksqlDB CLI and passes it a SQL
script for execution. The manual EXIT is required. The advantage of this
approach, compared with running ksqlDB Server headless with a queries
file, is that you can still interact with ksqlDB, and you can pre-build
the environment to a desired state.

```yaml
ksql-cli:
  image: confluentinc/ksqldb-cli:{{ site.ksqldbversion }}
  depends_on:
    - ksql-server
  volumes:
    - $PWD/ksql-scripts/:/data/scripts/
  entrypoint: 
    - /bin/bash
    - -c
    - |
      echo -e "\n\n⏳ Waiting for ksqlDB to be available before launching CLI\n"
      while [ $$(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip>:8088/) -eq 000 ]
      do 
        echo -e $$(date) "ksqlDB Server HTTP state: " $$(curl -s -o /dev/null -w %{http_code} http://<ksql-server-ip>:8088/) " (waiting for 200)"
        sleep 5
      done
      echo -e "\n\n-> Running SQL commands\n"
      cat /data/scripts/my-ksql-script.sql <(echo -e '\nEXIT')| ksql http://<ksql-server-ip>:8088
      echo -e "\n\n-> Sleeping…\n"
      sleep infinity
```
