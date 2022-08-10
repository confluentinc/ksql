---
layout: page
title: High Availability
tagline: Configure ksqlDB for high availability 
description: Learn about configuration settings for highly available ksqlDB queries 
---

# High availability

When you run pull queries, it’s often the case that you need your data to
remain available for querying even if one server fails. Because ksqlDB
supports clustering, it can remain highly available to support pull queries
on replicas of your data, even in the face of partial cluster failures.

High availability is turned off by default, but you can enable it with the
following server configuration parameters. These parameters must be turned on
for all nodes in your ksqlDB cluster.

1. Set [`ksql.streams.num.standby.replicas`](/reference/server-configuration/#ksqlstreamsnumstandbyreplicas) to a value greater than `0`.
1. Set [`ksql.query.pull.enable.standby.reads`](/reference/server-configuration/#ksqlquerypullenablestandbyreads) to `true`.
1. Set [`ksql.heartbeat.enable`](/reference/server-configuration/#ksqlheartbeatenable) to `true`.
1. Set [`ksql.lag.reporting.enable`](/reference/server-configuration/#ksqllagreportingenable) to `true`.

In addition, make sure that all of your nodes identify as part of the same
cluster by setting [`ksql.service.id`](/reference/server-configuration/#ksqlserviceid)
to the same value.

!!! note

    In {{ site.ccloud }}, ksqlDB clusters with 8 or 12 CSUs are automatically
    configured for high availability. High availability can't be enabled
    for {{ site.ccloud }} clusters with fewer than 8 CSUs.

## Controlling consistency

Because ksqlDB replicates data between its servers asynchronously, you may want
to bound the potential staleness that your query will tolerate. You can control
this per pull query by using the
[`ksql.query.pull.max.allowed.offset.lag`](/reference/server-configuration/#ksqlquerypullmaxallowedoffsetlag)
parameter. For instance, a value of `10,000` means that results of pull queries
forwarded to servers whose current offset is more than `10,000` positions
behind the end offset of the changelog topic are rejected.

## Compatability with Authentication
Set the `authentication.skip.paths` config with both `/lag` and `/heartbeat`.
This enables ksqlDB cluster instances to communicate without authenticating between each other.


This configuration prevents the following error.
```
ksqldb-server1     | [...] ERROR Failed to handle request 401 /heartbeat (io.confluent.ksql.api.server.FailureHandler:38)
ksqldb-server1     | io.confluent.ksql.api.server.KsqlApiException: Unauthorized
```

For security reasons, this configuration is recommended to block the traffic from outside your cluster to those endpoints.

# Recommended Resources
- [Course: Inside ksqlDB High Availability](https://developer.confluent.io/learn-kafka/inside-ksqldb/high-availability/)
- [Highly Available, Fault-Tolerant Pull Queries in ksqlDB](https://www.confluent.io/blog/ksqldb-pull-queries-high-availability/)
