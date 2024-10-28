---
layout: page
title: ksqlDB High Availability Pull Queries
tagline: ksqlDB High Availability Pull Queries
description: Configure high availability pull queries in ksqlDB
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/high-availability-pull-queries.html';
</script>

## High Availability configuration reference

The following list shows the required configs for using high availability (HA).

- [`ksql.advertised.listener`](../reference/server-configuration.md#ksqladvertisedlistener)
- [`ksql.streams.num.standby.replicas`](../reference/server-configuration.md#ksqlstreamsnumstandbyreplicas)
- [`ksql.query.pull.enable.standby.reads`](../reference/server-configuration.md#ksqlquerypullenablestandbyreads)
- [`ksql.heartbeat.enable`](../reference/server-configuration.md#ksqlheartbeatenable)
- [`ksql.lag.reporting.enable`](../reference/server-configuration.md#ksqllagreportingenable)


## High Availability for pull queries

ksqlDB supports [pull queries](../developer-guide/ksqldb-reference/select-pull-query.md), which
you use to query materialized state that is stored while executing a 
[persistent query](../concepts/queries.md#persistent). This works without issue when all nodes in
your ksqlDB cluster are operating correctly, but what happens when a node storing that state goes 
down? First, you must start multiple nodes and make sure inter-node communication
is configured so that query forwarding works correctly:

```properties
listeners=http://0.0.0.0:8088
ksql.advertised.listener=http://host1.example.com:8088
```

The `ksql.advertised.listener` configuration specifies the URL that is propagated to other nodes for inter-node requests, so
it must be reachable from other hosts/pods in the cluster. Inter-node requests are critical in a
multi-node cluster. For more information, see [configuring listeners of a ksqlDB cluster](../installation/server-config/#configuring-listeners-of-a-ksqldb-cluster).

While waiting for a failed node to restart is one possibility, this approach may incur more downtime than you
want, and it may not be possible if there is a more serious failure. The other possibility is to have
replicas of the data, ready to go when they're needed. Fortunately, {{ site.kstreams }} provides a
mechanism to do this:

```properties
ksql.streams.num.standby.replicas=1
ksql.query.pull.enable.standby.reads=true
```

This first configuration tells {{ site.kstreams }} to use a separate task that operates 
independently of the active (writer) state store to build up a replica of the state. The
second config indicates that reading is allowed from the replicas (or _standbys_) if reading fails
from the active store.

This approach is sufficient to enable high availability for pull queries in ksqlDB, but it requires that every
request must try the active first. A better approach is to use a _heartbeating_ mechanism to 
detect failed nodes preemptively, before a pull query arrives, so the request can forward straight to a 
replica. Set the following configs to detect failed nodes preemptively.

```properties
ksql.heartbeat.enable=true
ksql.lag.reporting.enable=true
```

The first configuration enables heartbeating, which should improve the speed of request handling significantly
during failures, as described above. The second config allows for lag data of each of the standbys to be
collected and sent to the other nodes to make routing decisions. In this case, the lag is defined by how many
messages behind the active a given standby is. If ensuring freshness is a priority, you can provide a threshold in a pull 
query request to avoid the largest outliers:

 
```sql
SET 'ksql.query.pull.max.allowed.offset.lag'='100';
SELECT * FROM QUERYABLE_TABLE WHERE ID = 456;
```

This configuration causes the request to consider only standbys that are within 100 messages of the active
host.

With these configurations, you can introduce as much redundancy as you require and
ensure that your pull queries succeed with controlled lag and low latency.

!!! note 
    {{ site.ccloud }} is configured with HA enabled by default on clusters 8 CSUs or more.

## Compatability with Authentication
Set the `authentication.skip.paths` config with both `/lag` and `/heartbeat`.
This enables ksqlDB cluster instances to communicate without authenticating between each other.


This configuration prevents the following error.
```
ksqldb-server1     | [...] ERROR Failed to handle request 401 /heartbeat (io.confluent.ksql.api.server.FailureHandler:38)
ksqldb-server1     | io.confluent.ksql.api.server.KsqlApiException: Unauthorized
```

For security reasons, this configuration is recommended to block the traffic from outside your cluster to those endpoints.