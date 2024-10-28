---
layout: page
title: Install ksqlDB
tagline: Install ksqlDB on-premises
description: Learn how to install ksqlDB on-premises
keywords: ksql, install, docker, docker-compose, container, docker image, on-prem
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/installing.html';
</script>

ksqlDB and Docker containers
----------------------------

You can run ksqlDB locally by using Docker containers, and you can define a
ksqlDB application by creating a *stack* of containers. A stack is a group of
containers that run interrelated services. For more information on stacks, see
[Describing Apps Using Stack Files](https://docs.docker.com/get-started/part4/#describing-apps-using-stack-files).

The minimal ksqlDB stack has containers for {{ site.aktm }}, {{ site.zk }}, and
ksqlDB Server. More sophisticated ksqlDB stacks can have {{ site.sr }},
{{ site.kconnect }}, and other third-party services, like Elasticsearch.

Stacks that have {{ site.sr }} can use Avro- and Protobuf-encoded events in ksqlDB
applications. Without {{ site.sr }}, your ksqlDB applications can use only JSON
or delimited formats.  

!!! note
    ksqlDB Server can connect to a remote {{ site.ak }} cluster that isn't
    defined in a local stack. In this case, you can run ksqlDB in a standalone
    container and pass in the connection parameters on the command line.

Docker images for ksqlDB
------------------------

ksqlDB has a server component and a separate command-line interface (CLI)
component. Both components have their own Docker images.

Confluent maintains images on [Docker Hub](https://hub.docker.com/u/confluentinc)
for ksqlDB components.

- **[ksqldb-server](https://hub.docker.com/r/confluentinc/ksqldb-server/):**
  ksqlDB Server image
- **[ksqldb-cli](https://hub.docker.com/r/confluentinc/ksqldb-cli/):**
  ksqlDB command-line interface (CLI) image
- **[cp-zookeeper](https://hub.docker.com/r/confluentinc/cp-zookeeper):**
  {{ site.zk }} image (Community Version)
- **[cp-schema-registry](https://hub.docker.com/r/confluentinc/cp-schema-registry):**
  {{ site.sr }} image (Community Version)
- **[cp-kafka](https://hub.docker.com/r/confluentinc/cp-kafka):**
  {{ site.aktm }} image (Community Version)

Install ksqlDB and {{ site.aktm }} by starting a
[Docker Compose](https://docs.docker.com/compose/) stack that runs containers
based on these images.

The following sections show how to install Docker and use the docker-compose
tool to download and run the ksqlDB and related images.

Install Docker
--------------

Install the Docker distribution that's compatible with your operating system.

!!! important
    For macOS and Windows, Docker runs in a virtual machine, and you must
    allocate at least 8 GB of RAM for the Docker VM to run the {{ site.ak }}
    stack. The default is 2 GB.

- For macOS, use
  [Docker Desktop for Mac](https://docs.docker.com/desktop/mac/install/).
  Change the **Memory** setting on the
  [Resources](https://docs.docker.com/desktop/mac/#resources) page to 8 GB.
- For Windows, use
  [Docker Desktop for Windows](https://docs.docker.com/desktop/windows/install/).
  No memory change is necessary when you run Docker on
  [WSL 2](https://docs.docker.com/desktop/windows/install/#wsl-2-backend).
- For Linux, follow the [instructions](https://docs.docker.com/engine/install/)
  for your Linux distribution. No memory change is necessary, because Docker
  runs natively and not in a VM.

Create a stack file to define your ksqlDB application 
-----------------------------------------------------

When you've decided on the services that you want in the stack, you define a
[Compose file, or "stack" file](https://docs.docker.com/compose/compose-file/),
which is a YAML file, to configure your ksqlDB application's services. The 
stack file is frequently named `docker-compose.yml`. 

To start the ksqlDB application, use the
[docker-compose CLI](https://docs.docker.com/compose/) to
run the stack for the application. Run `docker-compose up` to start the
application and `docker-compose down` to stop it.

!!! note
    If the stack file is compatible with version 3 or higher,
    you can use the `docker stack deploy` command:
    `docker stack deploy -c docker-compose.yml your-ksqldb-app`.
    For more information, see
    [docker stack deploy](https://docs.docker.com/engine/reference/commandline/stack_deploy/).

Build a ksqlDB application
--------------------------

The following steps describe how to define and deploy a stack for a ksqlDB
application.

### 1. Define the services for your ksqlDB application

Decide which services you need for your ksqlDB application. 

For a local installation, include one or more {{ site.ak }} brokers in the
stack and one or more ksqlDB Server instances. 

- **{{ site.zk }}:** one instance, for cluster metadata
- **{{ site.ak }}:** one or more instances
- **{{ site.sr }}:** optional, but required for Avro, Protobuf, and JSON_SR
- **ksqlDB Server:** one or more instances
- **ksqlDB CLI:** optional
- **Other services:** like Elasticsearch, optional

!!! note
    A stack that runs {{ site.sr }} can handle Avro, Protobuf, and JSON_SR
    formats. Without {{ site.sr }}, ksqlDB handles only JSON or delimited
    schemas for events. 

You can declare a container for the ksqlDB CLI in the stack, or you can attach
the CLI to a ksqlDB Server instance later, from a separate container.

### 2.  Build the stack

Build a stack of services and deploy them by using
[Docker Compose](https://docs.docker.com/compose/).

Define the configuration of your local ksqlDB installation by creating a
[Compose file](https://docs.docker.com/compose/compose-file/), which by
convention is named `docker-compose.yml`.

### 3. Bring up the stack and run ksqlDB
   
To bring up the stack and run ksqlDB, use the
[docker-compose](https://docs.docker.com/compose/reference/overview/) tool,
which reads your `docker-compose.yml` file and runs containers for your
{{ site.ak }} and ksqlDB services. 

ksqlDB reference stack
----------------------

Many `docker-compose.yml` files exist for different configurations, and this
topic shows a simple stack that you can extend for your use cases. The
[ksqlDB reference stack](https://github.com/confluentinc/ksql/blob/master/docker-compose.yml)
brings up these services:

- **{{ site.zk }}:** one instance
- **{{ site.ak }}:** one broker
- **{{ site.sr }}:** enables Avro, Protobuf, and JSON_SR
- **ksqlDB Server:** two instances
- **ksqlDB CLI:** one container running `/bin/sh`

### 1. Clone the ksqlDB repository.

```bash
git clone https://github.com/confluentinc/ksql.git
cd ksql
```

### 2. Switch to the correct branch

Switch to the correct ksqlDB release branch.

```bash
git checkout {{ site.ksqldbversionpostbranch }}
```

### 3. Start the stack

Start the stack by using the `docker-compose up` command. Depending on your
network speed, this may take up to 5-10 minutes.

```bash
docker-compose up -d
```

!!! tip
    The `-d` option specifies detached mode, so containers run in the background.

Your output should resemble:

```
Creating network "ksql_default" with the default driver
Creating ksql_zookeeper_1 ... done
Creating ksql_kafka_1     ... done
Creating ksql_schema-registry_1 ... done
Creating primary-ksqldb-server  ... done
Creating ksqldb-cli               ... done
Creating additional-ksqldb-server ... done
```

Run the following command to check the status of the stack.

```bash
docker-compose ps
```

Your output should resemble:

```
          Name                      Command            State                 Ports
-------------------------------------------------------------------------------------------------
additional-ksqldb-server   /usr/bin/docker/run         Up      0.0.0.0:32768->8090/tcp
ksql_kafka_1               /etc/confluent/docker/run   Up      0.0.0.0:29092->29092/tcp, 9092/tcp
ksql_schema-registry_1     /etc/confluent/docker/run   Up      8081/tcp
ksql_zookeeper_1           /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 3888/tcp
ksqldb-cli                 /bin/sh                     Up
primary-ksqldb-server      /usr/bin/docker/run         Up      0.0.0.0:8088->8088/tcp
```

When all of the containers have the `Up` state, the ksqlDB stack is ready
to use.

Start the ksqlDB CLI
--------------------

When all of the services in the stack are `Up`, run the following command
to start the ksqlDB CLI and connect to a ksqlDB Server.

Run the following command to start the ksqlDB CLI in the running `ksqldb-cli`
container.

```bash
docker exec -it ksqldb-cli ksql http://primary-ksqldb-server:8088
```

After the ksqlDB CLI starts, your terminal should resemble the following.

```
                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =        The Database purpose-built       =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2020 Confluent Inc.

CLI v{{ site.ksqldbversion }}, Server v{{ site.ksqldbversion }} located at http://primary-ksql-server:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

With the ksqlDB CLI running, you can issue SQL statements and queries on the
`ksql>` command line.

!!! note
    The ksqlDB CLI connects to one ksqlDB Server at a time. The ksqlDB CLI
    doesn't support automatic failover to another ksqlDB Server.

### Stacks with ksqlDB CLI containers

Some stacks declare a container for the ksqlDB CLI but don't specify the
process that runs in the container. This kind of stack declares a generic
shell entry point: 

```yaml
entrypoint: /bin/sh
```

To interact with a CLI container that's defined this way, use the
`docker exec` command to start the `ksql` process within the container.

```bash
docker exec -it ksqldb-cli ksql http://<ksqldb-server-host>:<ksqldb-port>
```

### Run a ksqlDB CLI container

For stacks that don't declare a container for the ksqlDB CLI, use the
`docker run` command to start a new container from the `ksqldb-cli`
image.

```bash
docker run --network tutorials_default --rm --interactive --tty \
    confluentinc/ksqldb-cli:latest ksql \
    http://ksql-server:8088
```

The `--interactive` and `--tty` options together enable the ksqlDB CLI process
to communicate with the console. For more information, see
[docker run](https://docs.docker.com/engine/reference/run/#foreground).

Stop your ksqlDB application
----------------------------

Run the following command to stop the containers in your stack.

```bash
docker-compose down
```

Your output should resemble:

```
Stopping additional-ksqldb-server ... done
Stopping ksqldb-cli               ... done
Stopping primary-ksqldb-server    ... done
Stopping ksql_schema-registry_1   ... done
Stopping ksql_kafka_1             ... done
Stopping ksql_zookeeper_1         ... done
Removing additional-ksqldb-server ... done
Removing ksqldb-cli               ... done
Removing primary-ksqldb-server    ... done
Removing ksql_schema-registry_1   ... done
Removing ksql_kafka_1             ... done
Removing ksql_zookeeper_1         ... done
Removing network ksql_default
```

Specify ksqlDB Server configuration parameters
----------------------------------------------

You can specify the configuration for your ksqlDB Server instances by using
these approaches:

- **The `environment` key:** In the stack file, populate the `environment` key with
  your settings. By convention, the ksqlDB setting names are prepended with
  `KSQL_`.
- **`--env` option:** On the
  [docker run](https://docs.docker.com/engine/reference/commandline/run/)
  command line, specify your settings by using the `--env` option once for each
  parameter. For more information, see
  [Configure ksqlDB with Docker](install-ksqldb-with-docker.md).
- **ksqlDB Server config file:** Add settings to the `ksql-server.properties`
  file. This requires building your own Docker image for ksqlDB Server. For
  more information, see [Configuring ksqlDB Server](server-config/index.md).
 
For a complete list of ksqlDB parameters, see the
[Configuration Parameter Reference](/reference/server-configuration).

You can also set any property for the {{ site.kstreams }} API, the
{{ site.ak }} producer, or the {{ site.ak }} consumer.

A recommended approach is to configure a common set of properties
using the ksqlDB Server configuration file and override specific properties
as needed, using the environment variables.

ksqlDB must have access to a running {{ site.ak }} cluster, which can be on
your local machine, in a data center, a public cloud, or {{ site.ccloud }}.
For ksqlDB Server to connect to a {{ site.ak }} cluster, the required
parameters are `KSQL_LISTENERS` and `KSQL_BOOTSTRAP_SERVERS`, which have the
following default values:

```yaml
environment:
    KSQL_LISTENERS: http://0.0.0.0:8088
    KSQL_BOOTSTRAP_SERVERS: localhost:9092
```

ksqlDB runs separately from your {{ site.ak }} cluster, so you specify
the IP addresses of the cluster's bootstrap servers when you start a
container for ksqlDB Server. For more information, see
[Configuring ksqlDB Server](server-config/index.md).

To start ksqlDB containers in different configurations, see
[Configure ksqlDB with Docker](install-ksqldb-with-docker.md).

Supported versions and interoperability
---------------------------------------

You can use ksqlDB with compatible {{ site.aktm }} and {{ site.cp }}
versions.

|    ksqlDB version       | {{ site.ksqldbversion }} |
| ----------------------- | ------------------------ |
| {{ site.aktm }} version | 0.11.0 and later         |
| {{ site.cp }} version   | 5.5.0 and later          |

ksqlDB supports Java 8 and Java 11.

Scale your ksqlDB Server deployment
-----------------------------------

You can scale ksqlDB by adding more capacity per server (vertically) or by
adding more servers (horizontally). Also, you can scale ksqlDB clusters
during live operations without loss of data. For more information, see
[Scaling ksqlDB](../capacity-planning.md#scaling-ksqldb).

The ksqlDB servers are run separately from the ksqlDB CLI client and {{ site.ak }}
brokers. You can deploy servers on remote machines, VMs, or containers,
and the CLI connects to these remote servers.

![image](../../img/client-server.png)

You can add or remove servers from the same resource pool during live
operations, to scale query processing. You can use different resource pools
to support workload isolation. For example, you could deploy separate pools
for production and for testing.

Next Steps
----------

### Configure ksqlDB for Confluent Cloud

You can use ksqlDB with a {{ site.ak }} cluster hosted in {{ site.ccloud }}.
For more information, see
[Connect ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/cp-component/ksql-cloud-config.html).

### Experiment with other stacks

You can try out other stacks that have different configurations, like the
"Quickstart" and "PostgreSQL" stacks.

#### ksqlDB Quickstart stack

Download the `docker-compose.yml` file from the **Include Kafka** tab of the
[ksqlDB Quickstart](https://ksqldb.io/quickstart.html).

This `docker-compose.yml` file defines a stack with these features:

- Start one ksqlDB Server instance.
- Does not start {{ site.sr }}, so Avro and Protobuf schemas aren't available.
- Start the ksqlDB CLI container automatically.

Use the following command to start the ksqlDB CLI in the running `ksqldb-cli`
container.

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

#### Full ksqlDB event processing application

[The Confluent Platform Demo](https://github.com/confluentinc/cp-demo/)
shows how to build an event streaming application that processes live edits to
real Wikipedia pages. The
[docker-compose.yml](https://github.com/confluentinc/cp-demo/blob/master/docker-compose.yml)
file shows how to configure a stack with these features:

- Start a {{ site.ak }} cluster with two brokers.
- Start a {{ site.kconnect }} instance.
- Start {{ site.sr }}. 
- Start containers running Elasticsearch and Kibana.
- Start ksqlDB Server and ksqlDB CLI containers.

!!! note
    You must install
    [Confluent Platform](https://docs.confluent.io/current/installation/docker/installation/index.html)
    to run this application. The {{ site.cp }} images are distinct from the
    images that are used in this topic.

#### Confluent examples repo

There are numerous other stack files to explore in the
[Confluent examples repo](https://github.com/confluentinc/examples).

!!! note
    You must install
    [Confluent Platform](https://docs.confluent.io/current/installation/docker/installation/index.html)
    to run these applications. The {{ site.cp }} images are distinct from the
    images that are used in this topic.
