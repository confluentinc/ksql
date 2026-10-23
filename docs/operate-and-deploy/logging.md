---
layout: page
title: Logging
tagline: Log debug output from ksqlDB
description: Get DEBUG and INFO output from ksqlDB Server
keywords: log, logging, appender
---

# Logging

To get `DEBUG` or `INFO` output from ksqlDB Server, configure a {{ site.ak }}
appender for the server logs. Assign the following configuration settings in
the ksqlDB Server config file.

```yaml
Appenders:
    Kafka:
      name: kafka_appender
      topic: KSQL_LOG
      syncSend: true
      ignoreExceptions: false
      StructuredJsonLayout:
        Property:
          - name: schemas.enable
            value: false
      Property:
        - name: bootstrap.servers
          value: localhost:9092
Loggers:
  - name: io.confluent.ksql
    level: info
    AppenderRef:
      - ref: kafka_appender
```