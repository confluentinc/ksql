---
layout: page
title: Install ksqlDB
tagline: Install ksqlDB on-premises
description: Learn how to install ksqlDB on-premises
keywords: ksql, install, docker, docker-compose, container, docker image, on-prem
---

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

- [ksqldb-server](https://hub.docker.com/r/confluentinc/ksqldb-server/):
  ksqlDB Server image
- [ksqldb-cli](https://hub.docker.com/r/confluentinc/ksqldb-cli/):
  ksqlDB command-line interface (CLI) image
- [cp-zookeeper](https://hub.docker.com/r/confluentinc/cp-zookeeper):
  {{ site.zk }} image (Community Version)
- [cp-schema-registry](https://hub.docker.com/r/confluentinc/cp-schema-registry):
  {{ site.sr }} image (Community Version)
- [cp-kafka](https://hub.docker.com/r/confluentinc/cp-kafka):
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
  [Docker Desktop for Mac](https://docs.docker.com/docker-for-mac/install/).
  Change the **Memory** setting on the
  [Resources](https://docs.docker.com/docker-for-mac/#resources) page to 8 GB.
- For Windows, use
  [Docker Desktop for Windows](https://docs.docker.com/docker-for-windows/install/).
  Change the **Memory** setting on the
  [Advanced](https://docs.docker.com/docker-for-windows/#advanced) settings
  page to 8 GB.
- For Linux, follow the [instructions](https://docs.docker.com/install/)
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

### 1.  Define the services for your ksqlDB application

Decide which services you need for your ksqlDB application. 

For a local installation, include one or more {{ site.ak }} brokers in the
stack and one or more ksqlDB Server instances. 

- {{ site.zk }} -- one, for cluster metadata
- {{ site.ak }} -- one or more
- {{ site.sr }} -- optional, but required for Avro and Protobuf
- ksqlDB Server  -- one or more
- ksqlDB CLI -- optional
- Other services -- like Elasticsearch, optional

!!! note
    A stack that runs {{ site.sr }} can handle Avro- and Protobuf-encoded events.
    Without {{ site.sr }}, ksqlDB handles only JSON or delimited schemas for events. 

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

ksqlDB Tutorial stack
---------------------

Many `docker-compose.yml` files exist for different configurations, and this
topic shows a simple stack that you can extend for your use cases. The
stack for the [ksqlDB Tutorial](../../tutorials/basics-docker.md) brings up
these services:

- {{ site.zk }}
- {{ site.ak }} -- one broker
- {{ site.sr }} -- enables Avro and Protobuf
- ksqlDB Server -- one instance

Download the [docker-compose.yml file](https://github.com/confluentinc/ksql/blob/master/docs-md/tutorials/docker-compose.yml)
for the [ksqlDB Tutorial](../../tutorials/basics-docker.md) to get started with
a local installation of ksqlDB.

Start the stack
---------------

Navigate to the directory where you saved `docker-compose.yml` and start the
stack by using the `docker-compose up` command:

```bash
docker-compose up -d
```

!!! tip
    The `-d` option specifies detached mode, so containers run in the background.

Your output should resemble:

```
Creating network "tutorials_default" with the default driver
Creating tutorials_zookeeper_1 ... done
Creating tutorials_kafka_1     ... done
Creating tutorials_schema-registry_1 ... done
Creating tutorials_ksql-server_1     ... done
```

Run the following command to check the status of the stack.

```bash
docker-compose ps
```

Your output should resemble:

```
           Name                        Command            State                 Ports
----------------------------------------------------------------------------------------------------
tutorials_kafka_1             /etc/confluent/docker/run   Up      0.0.0.0:39092->39092/tcp, 9092/tcp
tutorials_ksql-server_1       /usr/bin/docker/run         Up
tutorials_schema-registry_1   /etc/confluent/docker/run   Up      8081/tcp
tutorials_zookeeper_1         /etc/confluent/docker/run   Up      2181/tcp, 2888/tcp, 3888/tcp
```

When all of the containers have the `Up` state, the ksqlDB stack is ready
to use.

Start the ksqlDB CLI
--------------------

When all of the services in the stack are `Up`, run the following command
to start the ksqlDB CLI and connect to a ksqlDB Server.

For the ksqlDB Tutorial stack, run the following command to start a container
from the `ksqldb-cli:latest` image that runs the ksqlDB CLI:

```bash
docker run --network tutorials_default --rm --interactive --tty \
    confluentinc/ksqldb-cli:latest ksql \
    http://ksql-server:8088
```

The `--interactive` and `--tty` options together enable the ksqlDB CLI process
to communicate with the console. For more information, see
[docker run](https://docs.docker.com/engine/reference/run/#foreground).

After the ksqlDB CLI starts, your terminal should resemble the following.

```
                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =  Event Streaming Database purpose-built =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2019 Confluent Inc.

CLI v{{ site.release }}, Server v{{ site.release }} located at http://ksql-server:8088

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
docker exec ksqldb-cli ksql http://<ksqldb-server-host>:<ksqldb-port>
```

Stop your ksqlDB application
----------------------------

Run the following command to stop the containers in your stack.

```bash
docker-compose down
```

Your output should resemble:

```
Stopping tutorials_ksql-server_1     ... done
Stopping tutorials_schema-registry_1 ... done
Stopping tutorials_kafka_1           ... done
Stopping tutorials_zookeeper_1       ... done
Removing tutorials_ksql-server_1     ... done
Removing tutorials_schema-registry_1 ... done
Removing tutorials_kafka_1           ... done
Removing tutorials_zookeeper_1       ... done
Removing network tutorials_default
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
[Configuration Parameter Reference](server-config/config-reference.md).

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

To start ksqlDB containers in configurations like "ksqlDB Headless Server"
and "ksqlDB Interactive Server (Development)", see
[Configure ksqlDB with Docker](install-ksqldb-with-docker.md).

Supported versions and interoperability
---------------------------------------

You can use ksqlDB with compatible {{ site.aktm }} and {{ site.cp }}
versions.

|    ksqlDB version     | {{ site.release }} |
| --------------------- | ------------------ |
| Apache Kafka version  | 0.11.0 and later   |
| {{ site.cp }} version | > 3.3.0 and later  |

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

Next steps
----------

### Configure ksqlDB for Confluent Cloud

You can use ksqlDB with a {{ site.ak }} cluster hosted in {{ site.ccloud }}.
For more information, see
[Connect ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html).

### Experiment with other stacks

You can try out other stacks that have different configurations, like the
"Quickstart" and "reference" stacks.

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

#### ksqlDB reference stack

Download the [docker-compose.yml file](https://github.com/confluentinc/ksql/blob/master/docker-compose.yml)
for the reference stack in the ksqlDB repo.

This `docker-compose.yml` file defines a stack with these features:

- Start two or more ksqlDB Server instances.
- Start {{ site.sr }}.
- Start the ksqlDB CLI container automatically. 
  
Use the following command to start the ksqlDB CLI in the running `ksqldb-cli`
container.

```bash
docker exec ksqldb-cli ksql http://primary-ksqldb-server:8088
```

#### PostgreSQL stack

The [ksqlDB with Embedded Connect](../../tutorials/embedded-connect.md) tutorial
shows how to integrate ksqlDB with an external PostgreSQL database to power a
simple ride sharing app. The `docker-compose.yml` file defines a stack with
these features:

- Start one ksqlDB Server instance.
- Start PostgreSQL on port 5432.
- Start the ksqlDB CLI container automatically.

Use the following command to start the ksqlDB CLI in the running `ksqldb-cli`
container.

```bash
docker exec ksqldb-cli ksql http://ksqldb-server:8088
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

Page last revised on: {{ git_revision_date }}
