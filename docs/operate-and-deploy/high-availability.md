# High availability

When you run pull queries, it’s often the case that you need your data to remain available for querying even if the server fails. Because ksqlDB supports clustering, it can remain highly available to support pull queries on replicas of your data, even in the face of partial cluster failure.

High availability is turned off by default, but you can enable it with the following server configuration parameters. These parameters must be turned on for all nodes in your ksqlDB cluster.

1. Set [`ksql.streams.num.standby.replicas`](/reference/server-configuration/#ksqlstreamsnumstandbyreplicas) to a value greater than `1`.
1. Set [`ksql.query.pull.enable.standby.reads`](/reference/server-configuration/#ksqlquerypullenablestandbyreads) to `true`.
1. Set [`ksql.heartbeat.enable`](/reference/server-configuration/#ksqlheartbeatenable) to `true`.
1. Set [`ksql.lag.reporting.enable`](/reference/server-configuration/#ksqllagreportingenable) to `true`.

## Controlling consistency

Because ksqlDB replicates data between its servers asynchronously, you may want to bound the potential staleness that your query will tolerate. You can control this per pull query using the [`ksql.query.pull.max.allowed.offset.lag`](/reference/server-configuration/#ksqlquerypullmaxallowedoffsetlag) parameter. For instance, a value of 10,000 means that results of pull queries forwarded to servers whose current offset is more than 10,000 positions behind the end offset of the changelog topic will be rejected.