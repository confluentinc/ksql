# Metrics

TODO: intro text.

TODO: move - Because ksqlDB persistent queries directly compile into {{
site.kstreams }} topologies, many ksqlDB metrics correspond to [Kafka
Streams metrics](https://docs.confluent.io/current/streams/monitoring.html).

## Engine

`io.confluent.ksql.metrics:type=ksql-engine-query-stats`

### Attributes

#### Number of persistent queries

`_confluent-ksql-default_num-persistent-queries`

The current number of persistent queries running in this engine.

#### Number of active queries

`_confluent-ksql-default_num-active-queries`

The current number of active queries running in this engine.

#### Number of running queries

`_confluent-ksql-default_ksql-engine-query-stats-RUNNING-queries`

Count of queries in `RUNNING` state.

#### Number of idle queries

`_confluent-ksql-default_num-idle-queries`

Number of inactive queries.

#### Number of not running queries

`_confluent-ksql-default_ksql-engine-query-stats-NOT_RUNNING-queries`

Count of queries in `NOT_RUNNING` state.

#### Number of rebalancing queries

`_confluent-ksql-default_ksql-engine-query-stats-REBALANCING-queries`

Count of queries in `REBALANCING` state.

#### Number of created queries

`_confluent-ksql-default_ksql-engine-query-stats-CREATED-queries`

Count of queries in `CREATED` state.

#### Number of pending shutdown queries

`_confluent-ksql-default_ksql-engine-query-stats-PENDING_SHUTDOWN-queries`

Count of queries in `PENDING_SHUTDOWN` state.

#### Number of error queries

`_confluent-ksql-default_ksql-engine-query-stats-ERROR-queries`

Count of queries in `ERROR` state.

#### Total bytes consumed

`_confluent-ksql-default_bytes-consumed-total`

The total number of bytes consumed across all queries.

#### Minimum messages consumed

`_confluent-ksql-default_messages-consumed-min`

Min msgs consumed by query.

#### Maximum messages consumed

`_confluent-ksql-default_messages-consumed-max`

Max msgs consumed by query.

#### Average messages consumed

`_confluent-ksql-default_messages-consumed-avg`

Mean msgs consumed by query.

#### Messages consumed per second

`_confluent-ksql-default_messages-consumed-per-sec`

The number of messages consumed per second across all queries.

#### Messages consumed total

`_confluent-ksql-default_messages-consumed-total`

The total number of messages consumed across all queries.

#### Messages produced per second

`_confluent-ksql-default_messages-produced-per-sec`

The number of messages produced per second across all queries.

#### Error rate

`_confluent-ksql-default_error-rate`

The number of messages which were consumed but not processed. Messages may not be processed if, for instance, the message contents could not be deserialized due to an incompatible schema. Alternately, a consumed messages may not have been produced, hence being effectively dropped. Such messages would also be counted toward the error rate.

#### Liveness indicator

`_confluent-ksql-default_liveness-indicator`

A metric with constant value `1` indicating the server is up and emitting metrics.

## Pull queries

TODO: `io.confluent.ksql.metrics:type=_confluent-ksql-default_pull-query`

TODO: explain enabling `ksql.query.pull.metrics.enabled=true`

### Attributes

#### Pull query total requests

`pull-query-requests-total`

Total number of pull query requests.

#### Pull query request rate

`pull-query-requests-rate`

Rate of pull query requests.

#### Pull query requests error count

`pull-query-requests-error-total`

Total number of erroneous pull query requests.

#### Pull query request error rate

`pull-query-requests-error-rate`

Rate of erroneous pull query requests.

#### Local pull query requests count

`pull-query-requests-local`

Count of local pull query requests.

#### Local pull query requests rate

`pull-query-requests-local-rate`

Rate of local pull query requests.

#### Remote pull query requests count

`pull-query-requests-remote`

Count of remote pull query requests.

#### Remote pull query requests rate

`pull-query-requests-remote-rate`

Rate of remote pull query requests.

#### Pull query minimum request latency

`pull-query-requests-latency-latency-min`

Average time for a pull query request.

#### Pull query maximum request latency

`pull-query-requests-latency-latency-max`

Max time for a pull query request.

#### Pull query average request latency

`pull-query-requests-latency-latency-avg`

Average time for a pull query request.

#### Pull query latency 50th percentile

`pull-query-requests-latency-distribution-50`

Latency distribution of the 50th percentile.

#### Pull query latency 75th percentile

`pull-query-requests-latency-distribution-75`

Latency distribution of the 75th percentile.

#### Pull query latency 75th percentile

`pull-query-requests-latency-distribution-90`

Latency distribution of the 90th percentile.

#### Pull query latency 99th percentile

`pull-query-requests-latency-distribution-99`

Latency distribution of the 99th percentile.
