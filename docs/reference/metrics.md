# Metrics

TODO: intro text.

TODO: move - Because ksqlDB persistent queries directly compile into {{
site.kstreams }} topologies, many ksqlDB metrics correspond to [Kafka
Streams metrics](https://docs.confluent.io/current/streams/monitoring.html).

## Engine metrics

### `error-rate`

The number of messages which were consumed but not processed. Messages may not be processed if, for instance, the message contents could not be deserialized due to an incompatible schema. Alternately, a consumed messages may not have been produced, hence being effectively dropped. Such messages would also be counted toward the error rate.

### `messages-produced-per-sec`

The number of messages produced per second across all queries.

### `messages-consumed-per-sec`

The number of messages consumed per second across all queries.

### `messages-consumed-total`

The total number of messages consumed across all queries.

### `bytes-consumed-total`

The total number of bytes consumed across all queries.

### `num-active-queries`

The current number of active queries running in this engine.

### `num-persistent-queries`

The current number of persistent queries running in this engine.

### `num-idle-queries`

Number of inactive queries.

### `liveness-indicator`

A metric with constant value 1 indicating the server is up and emitting metrics.

### `messages-consumed-max`

Max messages consumed by query.

### `messages-consumed-min`

Min msgs consumed by query.

### `messages-consumed-avg`

Mean msgs consumed by query.

## Pull query metrics

### `pull-query-pull-query-requests-local`

Count of local pull query requests.

### `pull-query-pull-query-requests-local-rate`

Rate of local pull query requests.

### `pull-query-pull-query-requests-remote`

Count of remote pull query requests.

### `pull-query-pull-query-requests-remote-rate`

Rate of remote pull query requests.

### `pull-query-pull-query-requests-error-rate`

Rate of erroneous pull query requests.

### `pull-query-pull-query-requests-error-total`

Total number of erroneous pull query requests.

### `pull-query-pull-query-requests-latency-latency-avg`

Average time for a pull query request.

### `pull-query-pull-query-requests-latency-latency-min`

Average time for a pull query request.

### `pull-query-pull-query-requests-latency-latency-max`

Max time for a pull query request.

### `pull-query-pull-query-requests-latency-latency-total`

Total number of pull query request.

### `pull-query-pull-query-requests-latency-distribution-50`

Latency distribution of the 50th percentile.

### `pull-query-pull-query-requests-latency-distribution-75`

Latency distribution of the 75th percentile.

### `pull-query-pull-query-requests-latency-distribution-90`

Latency distribution of the 90th percentile.

### `pull-query-pull-query-requests-latency-distribution-99`

Latency distribution of the 99th percentile.