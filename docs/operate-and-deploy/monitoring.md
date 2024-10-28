---
layout: page
title: Monitoring
tagline: Monitor ksqlDB
description: Enable metrics monitoring on ksqlDB Server 
keywords: metrics, monitor, jmx
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/monitoring.html';
</script>

# Monitoring

## Context

ksqlDB publishes metrics via JMX ([Java Management
Extensions](https://www.oracle.com/java/technologies/javase/javamanagement.html))
which help you monitor what is happening inside of ksqlDB's servers. For a
comprehensive list of metrics, see [the reference section](../reference/metrics.md).

## Enable monitoring

You must enable monitoring explicitly on each ksqlDB server. To enable
it in a Docker-based deployment, export an environment variable named
`KSQL_JMX_OPTS` with your JMX configuration and expose the port that
JMX will communicate over.

The following Docker Compose example shows how you can configure
monitoring for ksqlDB server. The surrounding components, like the
broker and CLI, are omitted for brevity. You can see an example of a
complete setup in the [quickstart](https://ksqldb.io/quickstart.html).

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
    - "1099:1099"
  environment:
    KSQL_LISTENERS: "http://0.0.0.0:8088"
    KSQL_BOOTSTRAP_SERVERS: "broker:9092"
    KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
    KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
    KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
    KSQL_KSQL_QUERY_PULL_METRICS_ENABLED: "true"
    KSQL_JMX_OPTS: >
      -Djava.rmi.server.hostname=localhost
      -Dcom.sun.management.jmxremote
      -Dcom.sun.management.jmxremote.port=1099
      -Dcom.sun.management.jmxremote.authenticate=false
      -Dcom.sun.management.jmxremote.ssl=false
      -Dcom.sun.management.jmxremote.rmi.port=1099
```

With respect to monitoring, here it what this does:

- The environment variable `KSQL_JMX_OPTS` is supplied to the server
  with various arguments. The `>` character lets you write a
  multi-line string in Yaml, which makes this long argument easier to
  read. The advertised hostname, port, and security settings are
  configured. JMX has a wide range of [configuration
  options](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html),
  and you can set these however you like.

- Port `1099` is exposed, which corresponds to the JMX port set in the
  `KSQL_JMX_OPTS` configuration. This enables remote monitoring tools
  to communicate into ksqlDB's process.

## Verifying your monitoring setup

An easy way to check that ksqlDB is properly emitting metrics is by
using `jconsole`. JConsole is a graphical monitoring tool to monitor
the JVM, and it is included in Oracle JDK installations.

On your host machine, run the command:

```bash
jconsole
```

You will be prompted for a host and port. If you used the example
configuration above, then entering `localhost:1099` will allow
JConsole to establish the connection. You should see a series of 
graphs showing resource utilization. If you don't, make sure the 
networking between your machine and the Docker container is 
configured correctly.
