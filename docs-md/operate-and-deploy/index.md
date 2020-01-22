---
layout: page
title: Operate and Deploy
tagline: Operate, administer, and deploy ksqlDB
description: Learn how to operate ksqlDB.
keywords: ksqldb, operate, administer, deploy, docker, install
---

KSQL versus ksqlDB
--------------------

Overview of key differences between [KSQL and ksqlDB](ksql-vs-ksqldb.md).

Install and configure ksqlDB
----------------------------

You have a number of options when you set up ksqlDB Server. For more
information on installing and configuring ksqlDB, see the following
topics.

- [Deploy](installation/install-ksqldb-with-docker.md)
- [Configure ksqlDB Server](installation/server-config/index.md)
- [Configure ksqlDB CLI](installation/cli-config.md)
- [Configure Security](installation/server-config/security.md)
- [ksqlDB Configuration Parameter Reference](installation/server-config/config-reference.md)

Health Checks
-------------

- The ksqlDB REST API supports a "server info" request at
  `http://<server>:8088/info` and a basic server health check endpoint at
  `http://<server>:8088/healthcheck`.
- Check runtime stats for the ksqlDB server that you are connected to
  via `DESCRIBE EXTENDED <stream or table>` and
  `EXPLAIN <name of query>`.
- Run `ksql-print-metrics` on a ksqlDB server. For example, see this
  [blog post](https://www.confluent.io/blog/ksql-january-release-streaming-sql-apache-kafka/).

Capacity Planning
-----------------

The [Capacity Planning guide](capacity-planning.md)
describes how to size your ksqlDB clusters.

Troubleshooting
---------------

If ksqlDB isn't behaving as expected, see
[Troubleshoot ksqlDB issues](../troubleshoot-ksqldb.md)

Monitoring and Metrics
----------------------

ksqlDB includes JMX (Java Management Extensions) metrics which give
insights into what is happening inside your ksqlDB servers. These metrics
include the number of messages, the total throughput, throughput
distribution, error rate, and more.

To enable JMX metrics, set `JMX_PORT` before starting the ksqlDB server:

```bash
export JMX_PORT=1099 && \
<path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties
```

The `ksql-print-metrics` command line utility collects these metrics and
prints them to the console. You can invoke this utility from your
terminal:

```bash
<path-to-confluent>/bin/ksql-print-metrics
```

Your output should resemble:

```
messages-consumed-avg: 96416.96196183885
messages-consumed-min: 88900.3329377909
error-rate: 0.0
num-persistent-queries: 2.0
messages-consumed-per-sec: 193024.78294586178
messages-produced-per-sec: 193025.4730374501
num-active-queries: 2.0
num-idle-queries: 0.0
messages-consumed-max: 103397.81191436431
```

For more information about {{ site.kstreams }} metrics, see
[Monitoring Streams Applications](https://docs.confluent.io/current/streams/monitoring.html).

Next Steps
----------

- Watch the
  [screencast of Taking KSQL to Production](https://www.youtube.com/embed/f3wV8W_zjwE).

Page last revised on: {{ git_revision_date }}