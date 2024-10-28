---
layout: page
title: Logging
tagline: Log debug output from ksqlDB
description: Get DEBUG and INFO output from ksqlDB Server
keywords: log, logging, appender
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/logging.html';
</script>

# Logging

To get `DEBUG` or `INFO` output from ksqlDB Server, configure a {{ site.ak }}
appender for the server logs. Assign the following configuration settings in
the ksqlDB Server config file.

```properties
log4j.appender.kafka_appender=org.apache.kafka.log4jappender.KafkaLog4jAppender
log4j.appender.kafka_appender.layout=io.confluent.common.logging.log4j.StructuredJsonLayout
log4j.appender.kafka_appender.BrokerList=localhost:9092
log4j.appender.kafka_appender.Topic=KSQL_LOG
log4j.logger.io.confluent.ksql=INFO,kafka_appender
```