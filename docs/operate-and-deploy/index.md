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

For more information, see
[Check the health of a ksqlDB Server](installation/check-ksqldb-server-health.md).

Capacity Planning
-----------------

The [Capacity Planning guide](capacity-planning.md)
describes how to size your ksqlDB clusters.

Troubleshooting
---------------

If ksqlDB isn't behaving as expected, see
[Troubleshoot ksqlDB issues](../troubleshoot-ksqldb.md)

Next Steps
----------

- Watch the
  [screencast of Taking KSQL to Production](https://www.youtube.com/embed/f3wV8W_zjwE).
