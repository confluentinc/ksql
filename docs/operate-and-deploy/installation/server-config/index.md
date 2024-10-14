---
layout: page
title: Configuring ksqlDB Server
tagline: Detailed settings reference
description: Learn about the configuration settings you can use to set up ksqlDB
---

- [Configure Security for ksqlDB](security.md)
- [ksqlDB Configuration Parameter Reference](/reference/server-configuration)
- [Configure ksqlDB for Avro, Protobuf, and JSON schemas](avro-schema.md)

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
    [ksqlDB Server image](https://hub.docker.com/r/confluentinc/ksqldb-server/).
    For more information, see
    [Install ksqlDB with Docker](../install-ksqldb-with-docker.md).

### ksqlDB Server Configuration File

By default, the ksqlDB server configuration file is located at
`<path-to-confluent>/etc/ksqldb/ksql-server.properties`. The file follows
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
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksqldb/ksql-server.properties
```

For more information, see [ksqlDB Configuration Parameter Reference](/reference/server-configuration).

### KSQL_OPTS Environment Variable

You can override ksqlDB Server configuration parameters by using the
`KSQL_OPTS` environment variable. The properties are standard Java
system properties. For example, to set
`ksql.streams.num.streams.threads` to `1`:

```bash
KSQL_OPTS="-Dksql.streams.num.streams.threads=1" <path-to-confluent>/bin/ksql-server-start \
<path-to-confluent>/etc/ksqldb/ksql-server.properties
```

You can specify multiple parameters at the same time. For example, to
configure `ksql.streams.auto.offset.reset` and
`ksql.streams.num.stream.threads`:

```bash
KSQL_OPTS="-Dksql.streams.auto.offset.reset=earliest -Dksql.streams.num.stream.threads=1" <path-to-confluent>/bin/ksql-server-start \
<path-to-confluent>/etc/ksqldb/ksql-server.properties
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
[Headless Deployment](/operate-and-deploy/how-it-works#headless-deployment).

You can configure servers to exclusively run a predefined script (`.sql`
file) via the `--queries-file` command line argument, or the
`ksql.queries.file` setting in the
[ksqlDB configuration file](/reference/server-configuration). If a
server is running a predefined script, it will automatically disable its
REST endpoint and interactive use.

!!! note
    In headless mode, you must start all ksqlDB servers with the same queries
    file. If the queries files differ across ksqlDB servers, the behavior is
    undefined.

!!! tip
    When both the `ksql.queries.file` property and the `--queries-file`
    argument are present, the `--queries-file` argument takes
    precedence.

### Schema resolution

When you run a ksqlDB application that uses Avro or Protobuf, ksqlDB infers 
chemas from {{ site.sr }} automatically, but the behavior after restarting
ksqlDB Server differs between interactive and non-interactive mode.

- **Interactive mode:** after ksqlDB Server restarts, it doesn't contact
  {{ site.sr }} again to resolve schemas, because it has previously persisted
  the information to the command topic.
- **Non-interactive mode**: after ksqlDB Server restarts, it *does* contact
  {{ site.sr }} again to resolve schemas. If schemas have changed, unexpected
  behavior in your ksqlDB applications may occur.

!!! important
    If your ksqlDB applications use Avro or Protobuf, and you run them in non-interactive
    mode, ensure that the schemas don't change between ksqlDB Server restarts,
    or provide the schema explicitly. If the schema may evolve, it's safer to
    provide the schema explicitly.

### Start headless ksqlDB Server from the command line

To start the ksqlDB Server in headless, non-interactive configuration via the
`--queries-file` command line argument:

Create a predefined script and save as an `.sql` file.

Start the ksqlDB Server with the predefined script specified by using the
`--queries-file` argument.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksqldb/ksql-server.properties \
--queries-file /path/to/queries.sql
```

### Start headless ksqlDB Server by using the configuration file

To start the ksqlDB Server in headless, non-interactive configuration via the
`ksql.queries.file` in the server configuration file:

Configure the `ksql-server.properties` file. The
`bootstrap.servers` and `ksql.queries.file` are required. For
more information about configuration, see
[ksqlDB configuration file](/reference/server-configuration).

```properties
# Inform the ksqlDB server where the Kafka cluster can be found:
bootstrap.servers=localhost:9092

# Define the location of the queries file to execute
ksql.queries.file=/path/to/queries.sql
```

Start the ksqlDB server with the configuration file specified.

```bash
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksqldb/ksql-server.properties
```

Configuring Listeners of a ksqlDB Cluster
------------------------------------------

Multiple hosts are required to scale ksqlDB processing power and to do that, they must
form a cluster.  ksqlDB requires all hosts of a cluster to use the same `ksql.service.id`.

```properties
bootstrap.servers=kafkaBroker1:9092,kafkaBroker2:9092
ksql.service.id=my_application_
```

Once formed, many operations can be run using the client APIs exposed on `listeners`.

In order to utilize pull queries and their high availability functionality, the nodes within the
cluster must be able to communicate with each other. ksqlDB supports setups with either a single
shared listener for both client and internal communication, or dual single-purpose listeners. The
following section describes how to configure listeners depending on the nature of your environment and
requirements.

### Single listener

#### Single routable listener

If you want to configure your listener with an IP address or hostname that is resolvable and
routable from within the cluster, you might do the following:

```properties
# Hostname that other nodes can resolve to an routable IP:
listeners=https://ksqlHost56:8088

# Or, routable IP address:
listeners=https://192.168.1.101:8088
```

In this setup, the node shares the first URL in the `listeners` config as its internal
endpoint, which other nodes use for inter-node communication. Inter-node communication
uses the same listener as client communication.

#### Single non-routable listener

It's common to set up a service using special hostnames, like `localhost`, or wildcard addresses,
like `0.0.0.0` or `[::]`. These special hostnames have special meanings and are not appropriate for
inter-node communication, because they're not routable from other machines. This is also the case if your
network is set up such that the IP or hostname you bind isn't resolvable or routable.

If you choose to use a non-routable listener, you must set `ksql.advertised.listener` and specify a
URL that is externally accessible and which resolves to an endpoint defined in `listeners`.

```properties
# Non-routable wildcard ip:
listeners=http://0.0.0.0:8088

# Externally accessible name that resolves to the IP of the machine:
ksql.advertised.listener=http://host1.internal.example.com:8088
```

In this setup, the node shares the URL in the `ksql.advertised.listener` config as its
internal endpoint, which other nodes use for inter-node communication. Inter-node
communication uses the same listener as client communication.

### Dual listeners

You may choose to configure internal communication to use a different listener to client
communication, which enables port filtering rules to deny clients access to the internal listener or
the use of a different network interface for internal communication, for security or QoS reasons.

This can be achieved by setting the `ksql.internal.listener` configuration to start a second
listener that is used exclusively for inter-node communication.

If you're running dual listeners to improve security, you may also wish to enable
[authentication and other security measures](security.md).

#### Dual routable listeners

If the internal IP address or hostname used in the `ksql.internal.listener` configuration is externally
resolvable and routable, you only need to configure `ksql.internal.listener` to set the internal
listener, for example:

```properties
# Client listener:
listeners=https://192.168.1.101:8088

# Inter-node listener on different NIC:
ksql.internal.listener=https://192.168.1.102:8088
```

!!! note
    Only the `ksql.internal.listener` needs to be resolvable and routable from servers running other
    nodes in the cluster. The `listener` configuration can be non-resolvable and non-routable, because
    clients can connect using whatever URL you choose.

In this setup, the node shares the URL in the `ksql.internal.listener` config as its
internal endpoint, which other nodes use for inter-node communication. Inter-node
communication uses a different listener to client communication.

#### Dual non-routable listeners

If the internal IP address or hostname used in the `ksql.internal.listener` configuration is not
externally resolvable and routable, for example where it uses `localhost` or wildcard IPs such as
`0.0.0.0` or `[::]`, you must configure both `ksql.internal.listener` and `ksql.advertised.listener`
to set the internal listener:

```properties
# Client listener:
listeners=https://0.0.0.0:8088

# Inter-node listener on wildcard address and different port:
# Note: port 8099 could be locked down using port forward or other network tools.
ksql.internal.listener=https://0.0.0.0:8099

# URL that other nodes can resolve and use to route requests to this node:
ksql.advertised.listener=http://host1.internal.example.com:8099
```

!!! note
    Only the `ksql.advertised.listener` needs to be resolvable and routable from servers running
    other nodes in the cluster. The `listener` configuration can be non-resolvable and non-routable,
    because clients can connect using whatever URL you choose, and `ksql.internal.listener` is only used
    to start the listener.

In this setup, the node shares the url in the `ksql.advertised.listener` config as its
internal endpoint, which other nodes use for inter-node communication. Inter-node
communication uses a different listener to client communication.
