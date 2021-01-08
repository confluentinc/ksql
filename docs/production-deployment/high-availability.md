# High availability

Enable standby servers by setting ksql.streams.num.standby.replicas to a value greater or equal to 1.

Enable forwarding of pull queries to standby servers when the active is down by setting ksql.query.pull.enable.standby.reads to true.

Enable the heartbeating service so that servers can detect failure faster and provide a sub-second, real-time cluster membership by setting ksql.heartbeat.enable to true.

Enable the lag reporting service so that servers can filter results based on their inconsistency by setting ksql.lag.reporting.enable to true. Note that this feature does periodic (default: three seconds) RPC with Kafka brokers to obtain lag information and should be enabled only if you intend to configure tolerable inconsistency for pull queries, as explained below.