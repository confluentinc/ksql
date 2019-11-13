---
layout: page
title: Configuring ksqlDB Server
tagline: Detailed settings reference
description: Learn about the configuration settings you can use to set up ksqlDB
---

- [Configure Security for ksqlDB](security.md)
- [ksqlDB Configuration Parameter Reference](config-reference.md)
- [Configure Avro and {{ site.sr }} for ksqlDB](avro-schema.md)
- [Integrate ksqlDB with {{ site.c3 }}](integrate-ksql-with-confluent-control-center.md)

ksqlDB configuration parameters can be set for ksqlDB Server and for queries,
as well as for the underlying {{ site.kstreams }} and {{ site.ak }} Clients
(producer and consumer).

!!! tip
	These instructions assume you are installing {{ site.cp }} by using ZIP
    or TAR archives. For more information, see
    [On-Premises Deployments](https://docs.confluent.io/current/installation/installing_cp/index.html).

Setting ksqlDB Server Parameters
--------------------------------

You can specify ksqlDB Server configuration parameters by using the server
configuration file (`ksql-server.properties`) or the `KSQL_OPTS`
environment variable. Properties set with `KSQL_OPTS` take precedence
over those specified in the ksqlDB configuration file. A recommended
approach is to configure a common set of properties using the ksqlDB
configuration file and override specific properties as needed, using the
`KSQL_OPTS` environment variable.

!!! tip
	If you deploy {{ site.cp }} by using Docker containers, you can specify
    configuration parameters as environment variables to the
    [ksqlDB Server image](https://hub.docker.com/r/confluentinc/cp-ksql-server/). For more
    information, see [Install ksqlDB with Docker](../install-ksql-with-docker.md).

### ksqlDB Server Configuration File

By default, the ksqlDB server configuration file is located at
`<path-to-confluent>/etc/ksql/ksql-server.properties`. The file follows
the syntax conventions of
[Java properties files](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html).

```properties
<property-name>=<property-value>
```

For example:

```properties
bootstrap.servers=localhost:9092
listeners=http://localhost:8088
```

After you have updated the server configuration file, you can start the
ksqlDB Server with the configuration file specified.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties
```

For more information, see [ksqlDB Configuration Parameter Reference](config-reference.md).

### KSQL_OPTS Environment Variable

You can override ksqlDB Server configuration parameters by using the
`KSQL_OPTS` environment variable. The properties are standard Java
system properties. For example, to set
`ksql.streams.num.streams.threads` to `1`:

```bash
KSQL_OPTS="-Dksql.streams.num.streams.threads=1" <path-to-confluent>/bin/ksql-server-start \
<path-to-confluent>/etc/ksql/ksql-server.properties
```

You can specify multiple parameters at the same time. For example, to
configure `ksql.streams.auto.offset.reset` and
`ksql.streams.num.stream.threads`:

```bash
KSQL_OPTS="-Dksql.streams.auto.offset.reset=earliest -Dksql.streams.num.stream.threads=1" <path-to-confluent>/bin/ksql-server-start \
<path-to-confluent>/etc/ksql/ksql-server.properties
```

### ksqlDB Server Runtime Environment Variables

When ksqlDB Server starts, it checks for shell environment variables that
control the host Java Virtual Machine (JVM). Set the following
environment variables to control options like heap size and Log4j
configuration. These settings are applied by the
[ksql-run-class](https://github.com/confluentinc/ksql/blob/master/bin/ksql-run-class)
shell script when ksqlDB Server starts.

KSQL_CLASSPATH

:   Path to the Java deployment of ksqlDB Server and related Java classes.
    The following command shows an example KSQL_CLASSPATH setting.

    ```bash
    export CLASSPATH=/usr/share/java/my-base/*:/usr/share/java/my-ksql-server/*:/opt/my-company/lib/ksql/*:$CLASSPATH
    export KSQL_CLASSPATH="${CLASSPATH}"
    ```

KSQL_LOG4J_OPTS

:   Specifies ksqlDB Server logging options by using the Log4j
    configuration settings. The following example command sets the
    default Log4j configuration.

    ```bash
    export KSQL_LOG4J_OPTS="-Dlog4j.configuration=file:$KSQL_CONFIG_DIR/log4j-rolling.properties"
    ```

    For more information, see
    [Log4j Configuration](https://logging.apache.org/log4j/2.x/manual/configuration.html).

KSQL_JMX_OPTS

:   Specifies ksqlDB metrics options by using Java Management Extensions
    (JMX). The following example command sets the default JMX configuration.

    ```bash
    export KSQL_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false "
    ```

    For more information, see
    [Monitoring and Management Using JMX Technology](https://docs.oracle.com/en/java/javase/11/management/monitoring-and-management-using-jmx-technology.html).

KSQL_HEAP_OPTS

:   Specifies the initial size and maximum size of the JVM heap for the
    ksqlDB Server process. The following example command sets the initial
    size and maximum size to 15GB.

    ```bash
    export KSQL_HEAP_OPTS="-Xms15G -Xmx15G"
    ```

    For more information, see
    [JRockit JVM Heap Size Options](https://docs.oracle.com/cd/E15523_01/web.1111/e13814/jvm_tuning.htm#PERFM161).

KSQL_JVM_PERFORMANCE_OPTS

:   Specifies performance tuning options for the JVM that runs ksqlDB
    Server. The following example command sets the default JVM
    configuration.

    ```bash
    export KSQL_JVM_PERFORMANCE_OPTS="-server -XX:+UseConcMarkSweepGC -XX:+CMSClassUnload ingEnabled -XX:+CMSScavengeBeforeRemark -XX:+ExplicitGCInvokesConcurrent -XX:New Ratio=1 -Djava.awt.headless=true"
    ```

    For more information, see
    [D Command-Line Options](https://docs.oracle.com/en/java/javase/11/troubleshoot/command-line-options1.html).

JMX_PORT

:   Specifies the port that JMX uses to report metrics.

    ```bash
    export JMX_PORT=1099 
    ```

JAVA_HOME

:   Specifies the location of the `java` executable file.

    ```bash
    export JAVA_HOME=<jdk-install-directory>
    ```

JMX Metrics
-----------

To enable JMX metrics, set `JMX_PORT` before starting the ksqlDB server:

```bash
export JMX_PORT=1099 && \
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties
```

Run the `ksql-print-metrics` tool to see the available JMX metrics for
ksqlDB.

```bash
<path-to-confluent>/bin/ksql-print-metrics 
```

Your output should resemble:

```
    _confluent-ksql-default_bytes-consumed-total: 926543.0
    _confluent-ksql-default_num-active-queries: 4.0
    _confluent-ksql-default_ksql-engine-query-stats-RUNNING-queries: 4
    _confluent-ksql-default_ksql-engine-query-stats-NOT_RUNNING-queries: 0
    _confluent-ksql-default_messages-consumed-min: 0.0
    _confluent-ksql-default_messages-consumed-avg: 29.48784732897881
    _confluent-ksql-default_num-persistent-queries: 4.0
    _confluent-ksql-default_ksql-engine-query-stats-ERROR-queries: 0
    _confluent-ksql-default_num-idle-queries: 0.0
    _confluent-ksql-default_messages-consumed-per-sec: 105.07699698626074
    _confluent-ksql-default_messages-produced-per-sec: 11.256903025105757
    _confluent-ksql-default_error-rate: 0.0
    _confluent-ksql-default_ksql-engine-query-stats-PENDING_SHUTDOWN-queries: 0
    _confluent-ksql-default_ksql-engine-query-stats-REBALANCING-queries: 0
    _confluent-ksql-default_messages-consumed-total: 10503.0
    _confluent-ksql-default_ksql-engine-query-stats-CREATED-queries: 0
    _confluent-ksql-default_messages-consumed-max: 100.1243737430132
```

The following table describes the available ksqlDB metrics.

|        JMX Metric         |                                            Description                                             |
| ------------------------- | -------------------------------------------------------------------------------------------------- |
| bytes-consumed-total      | Number of bytes consumed across all queries.                                                       |
| error-rate                | Number of messages that have been consumed but not processed across all queries.                   |
| messages-consumed-avg     | Average number of messages consumed by a query per second.                                         |
| messages-consumed-per-sec | Number of messages consumed per second across all queries.                                         |
| messages-consumed-min     | Number of messages consumed per second for the query with the fewest messages consumed per second. |
| messages-consumed-max     | Number of messages consumed per second for the query with the most messages consumed per second.   |
| messages-consumed-total   | Number of messages consumed across all queries.                                                    |
| messages-produced-per-sec | Number of messages produced per second across all queries.                                         |
| num-persistent-queries    | Number of persistent queries that are currently executing.                                         |
| num-active-queries        | Number of queries that are actively processing messages.                                           |
| num-idle-queries          | Number of queries with no messages available to process.                                           |


Non-interactive (Headless) ksqlDB Usage
---------------------------------------

ksqlDB supports locked-down, "headless" deployment scenarios where
interactive use of the ksqlDB cluster is disabled. For example, the CLI
enables a team of users to develop and verify their queries
interactively on a shared testing ksqlDB cluster. But when you deploy
these queries in your production environment, you want to lock down
access to ksqlDB servers, version-control the exact queries, and store
them in a .sql file. This prevents users from interacting directly with
the production ksqlDB cluster. For more information, see
[Headless Deployment](../../concepts/ksql-architecture.md#headless-deployment).

You can configure servers to exclusively run a predefined script (`.sql`
file) via the `--queries-file` command line argument, or the
`ksql.queries.file` setting in the
[ksqlDB configuration file](config-reference.md). If a
server is running a predefined script, it will automatically disable its
REST endpoint and interactive use.

!!! tip
	When both the `ksql.queries.file` property and the `--queries-file`
    argument are present, the `--queries-file` argument takes
    precedence.

### Start headless ksqlDB Server from the command line

To start the ksqlDB Server in headless, non-interactive configuration via the
`--queries-file` command line argument:

Create a predefined script and save as an `.sql` file.

Start the ksqlDB Server with the predefined script specified by using the
`--queries-file` argument.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties \
--queries-file /path/to/queries.sql
```

### Start headless ksqlDB Server by using the configuration file

To start the ksqlDB Server in headless, non-interactive configuration via the
`ksql.queries.file` in the server configuration file:

Configure the `ksql-server.properties` file. The
`bootstrap.servers` and `ksql.queries.file` are required. For
more information about configuration, see
[ksqlDB configuration file](config-reference.md).

```properties
# Inform the ksqlDB server where the Kafka cluster can be found:
bootstrap.servers=localhost:9092

# Define the location of the queries file to execute
ksql.queries.file=/path/to/queries.sql
```

Start the ksqlDB server with the configuration file specified.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties
```

Page last revised on: {{ git_revision_date }}
