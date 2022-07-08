---
layout: page
title: Get the Status of the Servers in a ksqlDB Cluster 
tagline: clusterStatus endpoint
description: The `/clusterStatus` resource gives you the status of all servers in a ksqlDB cluster
keywords: ksqldb, server, status, cluster
---

The `/clusterStatus` resource gives you information about the status of all
ksqlDB servers in a ksqlDB cluster, which can be useful for troubleshooting. 
Enable this endpoint by setting [`ksql.heartbeat.enable`](../../reference/server-configuration.md#ksqlheartbeatenable) 
to `true`. Optionally, you can also set [`ksql.lag.reporting.enable`](../../reference/server-configuration.md#ksqllagreportingenable) 
to `true` to have your ksqlDB servers report state store lag, which will 
then also be returned with the response from the `/clusterStatus` endpoint.

!!! note
      ksqlDB servers in a cluster discover each other through persistent queries.
      If you have no persistent queries running, then the `/clusterStatus` endpoint
      contains info for the particular server that was queried, rather than
      all servers in the cluster.  

You can use the `curl` command to query the `/clusterStatus` endpoint
for a particular server:

```bash
curl -sX GET "http://localhost:8088/clusterStatus" | jq '.'
```

The response object contains a `clusterStatus` field with the following
information for each ksqlDB server (represented as `host:port`):

- **hostAlive** (boolean): whether the server is alive, as determined by
  heartbeats received by the queried server
- **lastStatusUpdateMs** (long): epoch timestamp, in milliseconds, for when the
  last status update was received for this server, by the queried server 
- **activeStandbyPerQuery** (object): for each query ID, a collection of 
  active and standby partitions and state stores on this server
- **hostStoreLags** (object): state store lag information. Empty unless
  `ksql.lag.reporting.enable` is set to `true`.
- **hostStoreLags.stateStoreLags** (object): partition-level lag breakdown
  for each state store.
- **hostStoreLags.updateTimeMs** (long): epoch timestamp, in milliseconds, for when 
  the last lag update was received for this server, by the queried server

For a two-node cluster running a single `CREATE TABLE ... AS SELECT` query, 
with lag reporting enabled, your output should resemble:

```json
{
  "clusterStatus": {
    "localhost:8088": {
      "hostAlive": true,
      "lastStatusUpdateMs": 1617609098808,
      "activeStandbyPerQuery": {
        "CTAS_MY_AGG_TABLE_3": {
          "activeStores": [
            "Aggregate-Aggregate-Materialize"
          ],
          "activePartitions": [
            {
              "topic": "my_stream",
              "partition": 1
            },
            {
              "topic": "my_stream",
              "partition": 3
            },
            {
              "topic": "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3-Aggregate-GroupBy-repartition",
              "partition": 1
            },
            {
              "topic": "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3-Aggregate-GroupBy-repartition",
              "partition": 3
            }
          ],
          "standByStores": [],
          "standByPartitions": []
        }
      },
      "hostStoreLags": {
        "stateStoreLags": {
          "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3#Aggregate-Aggregate-Materialize": {
            "lagByPartition": {
              "1": {
                "currentOffsetPosition": 0,
                "endOffsetPosition": 0,
                "offsetLag": 0
              },
              "3": {
                "currentOffsetPosition": 0,
                "endOffsetPosition": 0,
                "offsetLag": 0
              }
            },
            "size": 2
          }
        },
        "updateTimeMs": 1617609168917
      }
    },
    "other.ksqldb.host:8088": {
      "hostAlive": true,
      "lastStatusUpdateMs": 1617609172614,
      "activeStandbyPerQuery": {
        "CTAS_MY_AGG_TABLE_3": {
          "activeStores": [
            "Aggregate-Aggregate-Materialize"
          ],
          "activePartitions": [
            {
              "topic": "my_stream",
              "partition": 0
            },
            {
              "topic": "my_stream",
              "partition": 2
            },
            {
              "topic": "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3-Aggregate-GroupBy-repartition",
              "partition": 0
            },
            {
              "topic": "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3-Aggregate-GroupBy-repartition",
              "partition": 2
            }
          ],
          "standByStores": [],
          "standByPartitions": []
        }
      },
      "hostStoreLags": {
        "stateStoreLags": {
          "_confluent-ksql-default_query_CTAS_MY_AGG_TABLE_3#Aggregate-Aggregate-Materialize": {
            "lagByPartition": {
              "0": {
                "currentOffsetPosition": 1,
                "endOffsetPosition": 1,
                "offsetLag": 0
              },
              "2": {
                "currentOffsetPosition": 0,
                "endOffsetPosition": 0,
                "offsetLag": 0
              }
            },
            "size": 2
          }
        },
        "updateTimeMs": 1617609170111
      }
    }
  }
}
```

