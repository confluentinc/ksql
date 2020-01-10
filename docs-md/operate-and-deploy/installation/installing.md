---
layout: page
title: Install ksqlDB
tagline: Install ksqlDB on-premises
description: Learn how to install ksqlDB on-premises
keywords: ksql, install, docker, docker-compose, container, docker image, on-prem
---

ksqlDB and Docker containers
----------------------------

You can run ksqlDB locally by using Docker containers, and you define a ksqlDB
application by creating a *stack* of containers. A stack is a group of
containers that run interrelated services. For more information on stacks, see
[Describing Apps Using Stack Files](https://docs.docker.com/get-started/part4/#describing-apps-using-stack-files).

The minimal ksqlDB stack has containers for {{ site.aktm }}, {{ site.zk }}, and
ksqlDB Server. More sophisticated ksqlDB stacks can have {{ site.sr }},
{{ site.kconnect }}, and other third-party services, like Elasticsearch.

Stacks that have {{ site.sr }} enable using Avro-encoded events in your ksqlDB
applications. Without {{ site.sr }}, your ksqlDB applications can use only JSON
or delimited formats.  

!!! note
    ksqlDB Server can connect to a remote {{ site.ak }} cluster that isn't
    defined in a local stack. In this case, you can run ksqlDB in a standalone
    container and pass in the connection parameters on the command line.  

Docker images for ksqlDB
------------------------

ksqlDB has a server component and a separate command-line interface (CLI)
component, and both have their own Docker images.

Confluent maintains images on [Docker Hub](https://hub.docker.com/u/confluentinc)
for the ksqlDB components.

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

Install ksqlDB and {{ site.aktm }} by starting a Compose stack that runs
containers based on these images.

The following sections show how to install Docker and use the docker-compose
tool to download and run the ksqlDB and related images.



Create a stack file to define your ksqlDB application 
-----------------------------------------------------

When you've decided on the services that you want in the stack, you define a
[Compose file, or "stack" file](https://docs.docker.com/compose/compose-file/),
which is a YAML file, to configure your ksqlDB application's services. The 
stack file is frequently named `docker-compose.yml`. 

To start your ksqlDB application, you use the
[docker-compose CLI](https://docs.docker.com/compose/) to
run the stack for your application. Run `docker-compose up` to start your
application and `docker-compose down` to stop it.

!!! note
    If your stack file is compatible with version 3 or higher,
    you can use the `docker stack deploy` command:
    `docker stack deploy -c docker-compose.yml your-ksqldb-app`.
    For more information, see
    [docker stack deploy](https://docs.docker.com/engine/reference/commandline/stack_deploy/).

Build a ksqlDB application
--------------------------

The following steps show how to define and deploy a stack for a ksqlDB
application.

### 1.  Define the services for your ksqlDB application

Decide which services you need for your ksqlDB application. 

For a local installation, include one or more {{ site.ak }} brokers in the
stack and one or more ksqlDB Server instances. 

- {{ site.zk }}
- {{ site.ak }} -- one or more
- {{ site.sr }} -- optional, but required for Avro
- ksqlDB Server  -- one or more
- ksqlDB CLI -- optional

You can include a container for the ksqlDB CLI in the stack, or you can attach
the CLI to a ksqlDB Server instance later, from a separate container.


### 2.  Build a stack

Build a stack of services and deploy them by using
[Docker Compose](https://docs.docker.com/compose/).

### 3.  

Define the configuration of your local ksqlDB installation by creating a
[Compose file](https://docs.docker.com/compose/compose-file/), which by
convention is named `docker-compose.yml`.

3. Bring up the stack and run ksqlDB
   
To bring up the stack and run ksqlDB, use the
[docker-compose](https://docs.docker.com/compose/reference/overview/) tool,
which reads your `docker-compose.yml` file and runs containers for your
{{ site.ak }} and ksqlDB services. 

Many `docker-compose.yml` files exist for different configurations, and this
topic shows a few simple stacks that you can extend for your use cases. 


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

Choose a stack
--------------

The fastest way to get started with ksqlDB is to use a docker-compose file
that defines a ksqlDB stack with the necessary components: 

- {{ site.zk }}
- {{ site.ak }}
- {{ site.sr }} (optional)
- ksqlDB Server
- ksqlDB CLI

!!! note
    A stack that runs {{ site.sr }} can handle Avro-encoded events. Without
    {{ site.sr }}, ksqlDB handles only JSON or delimited schemas for events. 

You can use the following `docker-compose.yml` files to get started with a local
installation of ksqlDB.

### ksqlDB Quickstart stack

Download the `docker-compose.yml` file from the **Include Kafka** tab of the
[ksqlDB Quickstart](https://ksqldb.io/quickstart.html).

This `docker-compose.yml` file defines a stack with these features:

- Start one ksqlDB Server instance.
- Does not start {{ site.sr }}, so Avro schemas aren't available.
- Start the ksqlDB CLI container automatically.
- Command to start the ksqlDB CLI in the running container:
  `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`

### ksqlDB Tutorial stack

Download the [docker-compose.yml file](https://github.com/confluentinc/ksql/blob/master/docs/tutorials/docker-compose.yml)
for the [ksqlDB Tutorial](../../tutorials/basics-docker.md).

This `docker-compose.yml` file defines a stack with these features:

- Start one ksqlDB Server instance.
- Start {{ site.sr }}.
- You start the ksqlDB CLI container manually.

### ksqlDB reference stack

Download the [docker-compose.yml file](https://github.com/confluentinc/ksql/blob/master/docker-compose.yml)
for the reference stack in the ksqlDB repo.

This `docker-compose.yml` file defines a stack with these features:

- Start two or more ksqlDB Server instances.
- Start {{ site.sr }}.
- Start the ksqlDB CLI container automatically. 
- Command to start the ksqlDB CLI in the running container:
  `docker exec ksqldb-cli ksql http://primary-ksqldb-server:8088`

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

TODO: Compose output

When all of the containers have the `Up` status, the ksqlDB stack is ready
to use.

Start the ksqlDB CLI
--------------------

When all of the services in the stack are `Up`, run the following command
to start the ksqlDB CLI and connect to a ksqlDB Server.

For the ksqlDB reference stack, run the following command to start the ksqlDB
CLI process inside the running `ksqldb-cli` container.

```bash
docker exec ksqldb-cli ksql http://primary-ksqldb-server:8088
```

For the ksqlDB Quickstart stack, run the following command to start the ksqlDB
CLI process inside the running `ksqldb-cli` container.

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

For the ksqlDB Tutorial stack, run the following command to start a container
from the `ksqldb-cli:latest` image that runs the ksqlDB CLI:

```bash
docker run --network tutorials_default --rm --interactive --tty \
    confluentinc/ksqldb-cli:latest ksql \
    http://ksql-server:8088
```

!!! note
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

CLI v{{ site.release }}, Server v{{ site.release }} located at http://localhost:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

With the ksqlDB CLI running, you can issue SQL statements and queries on the
`ksql>` command line.





ksqlDB must have access to a running {{ site.ak }} cluster, which can
be in your data center, in a public cloud, or in {{ site.ccloud }}.

ksqlDB runs separately from your {{ site.ak }} cluster, so you specify
the IP addresses of the cluster's bootstrap servers when you start a
container for ksqlDB Server.

To start ksqlDB containers in configurations like "ksqlDB Headless Server"
and "ksqlDB Interactive Server (Development)", see
[Install ksqlDB with Docker](install-ksqldb-with-docker.md).

Supported Versions and Interoperability
---------------------------------------

You can use ksqlDB with compatible {{ site.aktm }} and {{ site.cp }}
versions.

|    ksqlDB version     | {{ site.release }} |
| --------------------- | ------------------ |
| Apache Kafka version  | 0.11.0 and later   |
| {{ site.cp }} version | > 3.3.0 and later  |

Scale Your ksqlDB Server Deployment
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

The ksqlDB CLI connects to only one ksqlDB Server at a time. The ksqlDB CLI
doesn't support automatic failover to another ksqlDB Server.

### Specify your ksqlDB server configuration parameters

Specify the configuration parameters for your ksqlDB server. You can also set
any property for the Kafka Streams API, the Kafka producer, or the Kafka
consumer. The required parameters are `bootstrap.servers` and `listeners`.
You can specify the parameters in the ksqlDB properties file or the `KSQL_OPTS`
environment variable. Properties set with `KSQL_OPTS` take precedence over
those specified in the properties file.

A recommended approach is to configure a common set of properties
using the ksqlDB configuration file and override specific properties
as needed, using the `KSQL_OPTS` environment variable.

Here are the default settings:

```
    bootstrap.servers=localhost:9092
    listeners=http://0.0.0.0:8088
```

For more information, see [Configuring ksqlDB Server](server-config/index.md).

Next Steps
----------

### Configure ksqlDB for Confluent Cloud

You can use ksqlDB with a {{ site.ak }} cluster hosted in {{ site.ccloud }}.
For more information, see
[Connect ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html).

### Full ksqlDB event processing application

[The Confluent Platform Demo](https://github.com/confluentinc/cp-demo/)
shows how to build an event streaming application that processes live edits to
real Wikipedia pages. The
[docker-compose.yml](https://github.com/confluentinc/cp-demo/blob/master/docker-compose.yml)
file shows how to configure a stack with these features:

- Start a {{ site.ak }} cluster with two brokers.
- Start a {{ site.kconnect }} instance.
- Start {{ site.sr }}. 
- Start containers running Elasticsearch and Kibana.
- Start ksqlDB Server and ksqlDB CLI. 

!!! note
    You need to install
    [Confluent Platform](https://docs.confluent.io/current/installation/docker/installation/index.html)
    to run this application. The {{ site.cp }} images are distinct from the
    images that are used in this topic.

There are numerous other Compose files to explore in the
[Confluent examples repo](https://github.com/confluentinc/examples).


Page last revised on: {{ git_revision_date }}
