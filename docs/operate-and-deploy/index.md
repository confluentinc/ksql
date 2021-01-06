---
layout: page
title: Operate and Deploy
tagline: Operate, administer, and deploy ksqlDB
description: Learn how to operate ksqlDB.
keywords: ksqldb, operate, administer, deploy, docker, install
---

<div class="cards">
  <div class="card operations">
    <strong>How it works</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>
</div>

<div class="cards">
  <div class="card operations">
    <strong>Clustering</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>

  <div class="card operations">
    <strong>High availability</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>
</div>

<div class="cards">
  <div class="card operations">
    <strong>Durability</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>

  <div class="card operations">
    <strong>Exactly-once semantics</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>
</div>

<div class="cards">
  <div class="card operations">
    <strong>Docker deployments</strong>
    <p class="card-body"><small>Create a table representing the latest values in a stream.</small></p>
    <a href="/how-to-guides/convert-changelog-to-table">Learn →</a>
  </div>

  <div class="card operations">
    <strong>Healthchecks</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>
</div>

<div class="cards">
  <div class="card operations">
    <strong>Security</strong>
    <p class="card-body"><small>Process and query structured data types like structs, maps, and arrays.</small></p>
    <a href="/how-to-guides/query-structured-data">Learn →</a>
  </div>

  <div class="card operations">
    <strong>Monitoring</strong>
    <p class="card-body"><small>Create a table representing the latest values in a stream.</small></p>
    <a href="/how-to-guides/convert-changelog-to-table">Learn →</a>
  </div>

  <div class="card operations">
    <strong>Capacity planning</strong>
    <p class="card-body"><small>Create a table representing the latest values in a stream.</small></p>
    <a href="/how-to-guides/convert-changelog-to-table">Learn →</a>
  </div>
</div>

<div class="cards">
  <div class="card operations">
    <strong>Logging</strong>
    <p class="card-body"><small>Create a table representing the latest values in a stream.</small></p>
    <a href="/how-to-guides/convert-changelog-to-table">Learn →</a>
  </div>

  <div class="card operations">
    <strong>Schema Registry integration</strong>
    <p class="card-body"><small>Create a table representing the latest values in a stream.</small></p>
    <a href="/how-to-guides/convert-changelog-to-table">Learn →</a>
  </div>
</div>

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