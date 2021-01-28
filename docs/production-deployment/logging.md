# Logging

- operational log and processing log. Can be routed differently
- former is for ops issues: is the cluster up and running?
- latter is info about what ksqlDB is doing as it processes records
- Ops log uses Log4j via a JVM variable to pass a logging config
- example logging configuration
- can't change log level at runtime

ksqlDB Server Log Settings
--------------------------

To get DEBUG or INFO output from ksqlDB Server, configure a {{ site.ak }}
appender for the server logs. Assign the following configuration settings in
the ksqlDB Server config file.

```properties
log4j.appender.kafka_appender=org.apache.kafka.log4jappender.KafkaLog4jAppender
log4j.appender.kafka_appender.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
log4j.appender.kafka_appender.BrokerList=localhost:9092
log4j.appender.kafka_appender.Topic=KSQL_LOG
log4j.logger.io.confluent.ksql=INFO,kafka_appender
```
