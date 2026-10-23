---
layout: page
title: Metrics
tagline: JMX metrics for monitoring ksqlDB
description: ksqlDB has many JMX metrics to monitor what its servers are doing 
keywords: metrics, jmx, monitor
---
# Metrics

ksqlDB emits a variety of JMX metrics to help you understand and
[monitor](../operate-and-deploy/monitoring.md) what its servers are
doing. This reference describes each metric and grouping.

## All persistent queries

Metrics that describe the full set of persistent queries on a given server.

```
io.confluent.ksql.metrics:type=_confluent-ksql-engine-query-stats,ksql_service_id=<ksql.service.id>
```

### Attributes

**Bytes consumed total**

`bytes-consumed-total`

The total number of bytes consumed across all queries.

**Created queries**

`created-queries`

The current number of created queries running in this engine.

**Error rate**

`error-rate`

The proportion of failed queries to successful queries, indicating messages
that were consumed but not processed. Messages may not be processed if, for
exammple, the message contents could not be deserialized due to an incompatible
schema. Alternatively, a consumed message may not have been produced, hence
being effectively dropped. Such messages would also be counted toward the error
rate. This can indicate problems with system configuration or issues in the 
queries being executed.

**Error queries**

`error-queries`

The number of queries that resulted in an error. This count can help identify
issues within the curent query set.

**Liveness indicator**

`liveness-indicator`

A metric with constant value `1` indicating the server is up and emitting metrics.

**Maximum messages consumed**

`messages-consumed-max`

The maximum number of messages consumed by all active queries.

**Messages consumed average**

`messages-consumed-avg`

The average number of messages consumed across all active queries. This can
indicate the average load on the system.

**Messages consumed per second**

`messages-consumed-per-sec`

The number of messages consumed per second across all queries. Higher values
can indicate a higher load on the system.

**Messages consumed total**

`messages-consumed-total`

The total number of messages consumed across all queries.

**Messages produced per second**

`messages-produced-per-sec`

The number of messages produced per second across all queries. This can
indicate system throughput.

**Minimum messages consumed**

`messages-consumed-min`

The minimum number of messages consumed by all active queries.

**Not-running queries**

`not-running-queries`

The number of queries that have been defined but are not currently running.

**Number of active queries**

`num-active-queries`

The current number of active queries running in this engine.

**Number of idle queries**

`num-idle-queries`

The number of queries that are currently idle, meaning that they are not 
processing any data.

**Number of persistent queries**

`num-persistent-queries`

The current number of persistent queries running in this engine.

**Pending shutdown queries**

`pending-shutdown-queries`

The number of queries that are in the process of shutting down, usually due to
a manual command to terminate.

**Rebalancing queries**

`rebalancing-queries`

The number of queries that are currently undergoing a rebalance operation.
Typically, this happens when a new worker joins a cluster or an existing one
leaves.

**Running queries**

`running-queries`

The current number of persistent queries running in this engine.

## Persistent query status

Metrics that describe the health of each persistent query.

```
io.confluent.ksql.metrics:type=ksql-queries
```

### Attributes

**ksqlDB query status**

`ksql-query-status`

The current ksqlDB status of the given query.   
The metric `query-status` shows the {{ site.kstreams }} application state.  
The `PAUSE` / `RESUME` commands do not impact the {{ site.kstreams }} state, so this new metric shows when a query is paused.

**Query status**

`query-status`

The current {{ site.kstreams }} status of the given query.  
The `ksql-query-status` metric has been added to show the ksqlDB query status.

**Error status**

`error-status`

The current error status of the given query, if the state is in ERROR state.

## Persistent query production

Metrics that describe the producer activity of each persistent query.

```
io.confluent.ksql.metrics:type=producer-metrics
```

### Attributes

**Total messages**

`total-messages`

The total number of messages produced.

**Messages per second**

`messages-per-sec`

The total number of messages produced per second.

## Persistent query consumption

Metrics that describe the consumer activity of each persistent query.

```
io.confluent.ksql.metrics:type=consumer-metrics
```

### Attributes

**Total messages**

`consumer-total-messages`

The total number of messages consumed.

**Messages per second**

`consumer-messages-per-sec`

The total number of messages consumed per second.

**Total bytes**

`consumer-total-bytes`

The total number of bytes consumed.

## Runtime

Because ksqlDB persistent queries directly compile into {{
site.kstreams }} topologies, many useful [{{ site.kstreams }}
metrics](https://docs.confluent.io/current/streams/monitoring.html)
are emitted for each persistent query. These metrics are omitted from
this reference to avoid redundancy.

## HTTP server

ksqlDB's REST API is built using Vert.x, and consequentially exposes
many [Vert.x
metrics](https://vertx.io/docs/vertx-dropwizard-metrics/java/)
directly. These metrics are omitted from this reference to avoid redundancy.


## Pull queries

Metrics that describe the activity of pull queries on each server.

```
io.confluent.ksql.metrics:type=_confluent-ksql-pull-query
```

!!! info
    Pull query metrics must be enabled explicitly by setting
    the `ksql.query.pull.metrics.enabled` server configuration to `true`.

### Attributes

**Pull query total requests**

`pull-query-requests-total`

Total number of pull query requests.

**Pull query request rate**

`pull-query-requests-rate`

Rate of pull query requests per second.

**Pull query requests error count**

`pull-query-requests-error-total`

Total number of erroneous pull query requests.

**Pull query request error rate**

`pull-query-requests-error-rate`

Rate of erroneous pull query requests per second.

**Local pull query requests count**

`pull-query-requests-local`

Count of local pull query requests.

**Local pull query requests rate**

`pull-query-requests-local-rate`

Rate of local pull query requests per second.

**Remote pull query requests count**

`pull-query-requests-remote`

Count of remote pull query requests.

**Remote pull query requests rate**

`pull-query-requests-remote-rate`

Rate of remote pull query requests per second.

**Pull query minimum request latency**

`pull-query-requests-latency-latency-min`

Average time for a pull query request.

**Pull query maximum request latency**

`pull-query-requests-latency-latency-max`

Max time for a pull query request.

**Pull query average request latency**

`pull-query-requests-latency-latency-avg`

Average time for a pull query request.

**Pull query latency 50th percentile**

`pull-query-requests-latency-distribution-50`

Latency distribution of the 50th percentile.

**Pull query latency 75th percentile**

`pull-query-requests-latency-distribution-75`

Latency distribution of the 75th percentile.

**Pull query latency 75th percentile**

`pull-query-requests-latency-distribution-90`

Latency distribution of the 90th percentile.

**Pull query latency 99th percentile**

`pull-query-requests-latency-distribution-99`

Latency distribution of the 99th percentile.

## User-defined functions

Metrics that describe the activity of user-defined functions, both in-built and custom added.

```
io.confluent.ksql.metrics:type=ksql-udf
```

!!! info
    UDF metrics must be enabled explicitly by setting
    the `ksql.udf.collect.metrics` server configuration to `true`.

### Attributes

ksqlDB creates a series of attributes per user-defined function.
The general form is:

```
ksql-(udf | udtf | udaf)-<function name>-(avg | count | max | rate)
```

For example, if you had a UDF named formula, you would see these attributes:

- `ksql-udf-formula-avg`
- `ksql-udf-formula-count`
- `ksql-udf-formula-max`
- `ksql-udf-formula-rate`

Here are what each of these suffixes mean.

**avg**

Average time for an invocation of the function.

**count**

Total number of invocations of the function.

**max**

Max time for an invocation of the function.

**rate**

The average number of invocations per second of the function.

## Command runner

Metrics that describe the health of the `CommandRunner` thread, which
enables each node to participate in distributed computation.

```
io.confluent.ksql.metrics:type=_confluent-ksql-rest-app-command-runner
```

### Attributes

**Thread status**

`status`

The status of the commandRunner thread as it processes the command topic.

## RocksDB

Metrics that report the resource utilization for RocksDB. If RocksDB runs out
of resources, it spools to disk, affecting performance. 

Run the `free -m` command to check for high cache usage. You may see that the
process is running at its configured memory threshold.

Also, you can check the following JMX metrics for high usage.

```
io.confluent.ksql.metrics:type=_ksql-rocksdb-aggregates
```

### Attributes

**Block cache usage**

`block-cache-usage`

Bytes allocated for the block cache. 

!!! note

    The `block-cache-usage` metric is distinct from the OS page cache reported
    by the `free -m` command.

**Current size of all memory tables**

`cur-size-all-mem-tables`

Amount of memory allocated for the write buffer.