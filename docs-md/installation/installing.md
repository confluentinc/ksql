---
layout: page
title: Install KSQL
tagline: Install KSQL on-premises
description: Learn how to install KSQL on-premises
keywords: ksql, install
---

Installing KSQL
===============

KSQL is a component of {{ site.cp }} and the KSQL binaries are located
at [Confluent Downloads](https://www.confluent.io/download/)
as a part of the {{ site.cp }} bundle.

KSQL must have access to a running {{ site.aktm }} cluster, which can
be in your data center, in a public cloud, {{ site.ccloud }}, etc.

Docker support
--------------

You can deploy KSQL by using
[Docker containers](install-ksql-with-docker.md).
Starting with {{ site.cp }} 4.1.2, Confluent maintains images at
[Docker Hub](https://hub.docker.com/r/confluentinc/cp-ksql-server/).
To start KSQL containers in configurations like "KSQL Headless
Server" and "Interactive Server with Interceptors", see [Docker
Configuration
Parameters](https://docs.confluent.io/current/installation/docker/config-reference.html).

Watch the [screencast of Installing and Running
KSQL](https://www.youtube.com/embed/icwHpPm-TCA) on YouTube.

Supported Versions and Interoperability
---------------------------------------

You can use KSQL with compatible {{ site.cp }} and {{ site.aktm }}
versions.

|     KSQL version      | {{ site.version }} |
| --------------------- | ------------------ |
| Apache Kafka version  | 0.11.0 and later   |
| {{ site.cp }} version | > 3.3.0 and later  |

Installation Instructions
-------------------------

Follow the instructions at [Confluent Platform Quick Start
(Local)](https://docs.confluent.io/current/quickstart/ce-quickstart.html).

Also, you can install KSQL individually by using the [confluent-ksql
package](https://docs.confluent.io/current/installation/available_packages.html#confluent-ksql).
For more information, see [Confluent Platform
Packages](https://docs.confluent.io/current/installation/available_packages.html).

Scale Your KSQL Server Deployment
---------------------------------

You can scale KSQL by adding more capacity per server (vertically) or by
adding more servers (horizontally). Also, you can scale KSQL clusters
during live operations without loss of data. For more information, see
[Scaling KSQL](../capacity-planning.md#scaling-ksql).

Start the KSQL Server
---------------------

The KSQL servers are run separately from the KSQL CLI client and Kafka
brokers. You can deploy servers on remote machines, VMs, or containers,
and the CLI connects to these remote servers.

You can add or remove servers from the same resource pool during live
operations, to scale query processing. You can use different resource pools
to support workload isolation. For example, you could deploy separate pools
for production and for testing.

You can only connect to one KSQL server at a time. The KSQL CLI does not
support automatic failover to another KSQL server.

![image](../img/client-server.png)

Follow these instructions to start KSQL Server using the
`ksql-server-start` script.

!!! tip
	These instructions assume you are installing {{ site.cp }} by using ZIP
    or TAR archives. For more information, see [On-Premises
    Deployments](https://docs.confluent.io/current/installation/installing_cp/index.html).

### Specify your KSQL server configuration parameters

Specify the configuration parameters for your KSQL server. You can also set
any property for the Kafka Streams API, the Kafka producer, or the Kafka
consumer. The required parameters are `bootstrap.servers` and `listeners`.
You can specify the parameters in the KSQL properties file or the `KSQL_OPTS`
environment variable. Properties set with `KSQL_OPTS` take precedence over
those specified in the properties file.

A recommended approach is to configure a common set of properties
using the KSQL configuration file and override specific properties
as needed, using the `KSQL_OPTS` environment variable.

Here are the default settings:

```
    bootstrap.servers=localhost:9092
    listeners=http://0.0.0.0:8088
```

For more information, see [Configuring KSQL Server](server-config/index.md).

### Start a KSQL server node

Start a server node by using the following command:

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties
```

!!! tip
	You can view the KSQL server help text by running
    `<path-to-confluent>/bin/ksql-server-start --help`.

    NAME
            server - KSQL Cluster

    SYNOPSIS
            server [ {-h | --help} ] [ --queries-file <queriesFile> ] [--]
                    <config-file>

    OPTIONS
            -h, --help
                Display help information

            --queries-file <queriesFile>
                Path to the query file on the local machine.

            --
                This option can be used to separate command-line options from the
                list of arguments (useful when arguments might be mistaken for
                command-line options)

            <config-file>
                A file specifying configs for the KSQL Server, KSQL, and its
                underlying Kafka Streams instance(s). Refer to KSQL documentation
                for a list of available configs.

                This option may occur a maximum of 1 times

Have a look at [this page](server-config/index.md#non-interactive-headless-ksql-usage)
for instructions on running KSQL in non-interactive (aka headless)
mode.

Start the KSQL CLI
------------------

The KSQL CLI is a client that connects to the KSQL servers.

You can start the KSQL CLI by providing the connection information to
the KSQL server.

```bash
LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql http://localhost:8088
```

!!! important
	By default KSQL attempts to store its logs in a directory called `logs`
    that is relative to the location of the `ksql` executable. For example,
    if `ksql` is installed at `/usr/local/bin/ksql`, then it would attempt
    to store its logs in `/usr/local/logs`. If you are running `ksql` from
    the default {{ site.cp }} location, `<path-to-confluent>/bin`, you must
    override this default behavior by using the `LOG_DIR` variable.

After KSQL is started, your terminal should resemble this.

```
===========================================
=        _  __ _____  ____  _             =
=       | |/ // ____|/ __ \| |            =
=       | ' /| (___ | |  | | |            =
=       |  <  \___ \| |  | | |            =
=       | . \ ____) | |__| | |____        =
=       |_|\_\_____/ \___\_\______|       =
=                                         =
=  Streaming SQL Engine for Apache Kafka® =
===========================================

Copyright 2019 Confluent Inc.

CLI v{{ site.release }}, Server v{{ site.release }} located at
<http://localhost:8088>

Having trouble? Type 'help' (case-insensitive) for a rundown of how
things work!

ksql>
```

!!! tip
		You can view the KSQL CLI help text by running
`<path-to-confluent>/bin/ksql --help`.

```
    NAME
            ksql - KSQL CLI

    SYNOPSIS
            ksql [ --config-file <configFile> ] [ {-h | --help} ]
                    [ --output <outputFormat> ]
                    [ --query-row-limit <streamedQueryRowLimit> ]
                    [ --query-timeout <streamedQueryTimeoutMs> ] [--] <server>

    OPTIONS
            --config-file <configFile>
                A file specifying configs for Ksql and its underlying Kafka Streams
                instance(s). Refer to KSQL documentation for a list of available
                configs.

            -h, --help
                Display help information

            --output <outputFormat>
                The output format to use (either 'JSON' or 'TABULAR'; can be changed
                during REPL as well; defaults to TABULAR)

            --query-row-limit <streamedQueryRowLimit>
                An optional maximum number of rows to read from streamed queries

                This options value must fall in the following range: value >= 1


            --query-timeout <streamedQueryTimeoutMs>
                An optional time limit (in milliseconds) for streamed queries

                This options value must fall in the following range: value >= 1


            --
                This option can be used to separate command-line options from the
                list of arguments (useful when arguments might be mistaken for
                command-line options)

            <server>
                The address of the Ksql server to connect to (ex:
                http://confluent.io:9098)

                This option may occur a maximum of 1 times
```

Configure KSQL for Confluent Cloud
----------------------------------

You can use KSQL with a Kafka cluster in {{ site.ccloud }}. For more
information, see
[Connect KSQL to Confluent Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html).

Page last revised on: {{ git_revision_date }}
