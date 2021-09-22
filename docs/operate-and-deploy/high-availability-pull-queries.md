---
layout: page
title: ksqlDB High Availability Pull Queries
tagline: ksqlDB High Availability Pull Queries
description: Instructions on how to set up high availability pull queries
---

HA Configuration Reference
--------------------------

The required configs for using high availability are summarized here:

- [`ksql.advertised.listener`](../reference/server-configuration.md#ksqladvertisedlistener)
- [`ksql.streams.num.standby.replicas`](../reference/server-configuration.md#ksqlstreamsnumstandbyreplicas)
- [`ksql.query.pull.enable.standby.reads`](../reference/server-configuration.md#ksqlquerypullenablestandbyreads)
- [`ksql.heartbeat.enable`](../reference/server-configuration.md#ksqlheartbeatenable)
- [`ksql.lag.reporting.enable`](../reference/server-configuration.md#ksqllagreportingenable)


High Availability for Pull Queries
----------------------------------

ksqlDB supports [pull queries](../developer-guide/ksqldb-reference/select-pull-query.md) which are
used to query materialized state that is stored while executing a 
[persistent query](../concepts/queries.md#persistent). This works without issue when all nodes in
your ksqlDB cluster are operating correctly, but what happens when a node storing that state goes 
down? To start with, you must first start multiple nodes and make sure inter-node communication
is configured so that query forwarding works correctly:

```properties
listeners=http://0.0.0.0:8088
ksql.advertised.listener=http://host1.example.com:8088
```

The latter configuration is the url propagated to other nodes for inter-node requests, so
it must be reachable from other hosts/pods in the cluster. Inter-node requests are critical in a
multi-node cluster and can be read about more [here](installation/server-config/index.md#configuring-listeners-of-a-ksqldb-cluster).

While waiting for a failed node to restart is one possibility, it may incur more downtime than you
want and it may not be possible if there is a more serious failure. The other possibility is to have
replicas of the data, ready to go when they're needed. Fortunately, {{ site.kstreams }} provides a
mechanism to do this:

```properties
ksql.streams.num.standby.replicas=1
ksql.query.pull.enable.standby.reads=true
```

This first configuration tells {{ site.kstreams }} that we want a separate task which will operate 
independently of the active (writer) state store to build up a replica of the state. The
second indicates that we want to allow reading from the replicas (a.k.a standbys) if we fail to read
from the active.

This is sufficient to allow for high availability pull queries in ksqlDB, but it requires every
request to try the active first. A better approach is to use a heartbeating mechanism to 
preemptively catch failed nodes before a pull query arrives, so it can forward straight to a 
replica. That can be done as follows:

```properties
ksql.heartbeat.enable=true
ksql.lag.reporting.enable=true
```

The first configuration does heartbeating, which should significantly speed up request handling
during failures, as described above. The second allows for lag data of each of the standbys to be
collected and sent to the other nodes to make routing decisions. The lag in this case is how many
messages behind the active a given standby is. The user can even provide a threshold in a pull 
query request to avoid the greatest outliers if ensuring freshness is a priority:

 
```sql
SET 'ksql.query.pull.max.allowed.offset.lag'='100';
SELECT * FROM QUERYABLE_TABLE WHERE ID = 456;
```

This will cause the request to only consider standbys that are within 100 messages of the active
host.

With these configurations, you should be able to introduce as much redundancy as you require and
ensure that your pull queries succeed with controlled lag and low latency.

!!! note 
    {{ site.ccloud }} is configured with HA enabled by default on clusters 8 CSUs or more.
 