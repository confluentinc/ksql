---
layout: page
title: Get the Status of a KSQL Server
tagline: info endpoint
description: The `/info` resource gives you the status of a KSQL server
keywords: ksqlDB, terminate
---

Get the Status of a KSQL Server
===============================

The `/info` resource gives you information about the status of a KSQL
server, which can be useful for health checks and troubleshooting. You
can use the `curl` command to query the `/info` endpoint:

```bash
curl -sX GET "http://localhost:8088/info" | jq '.'
```

Your output should resemble:

```json
{
  "KsqlServerInfo": {
    "version": "{{ site.release }}",
    "kafkaClusterId": "j3tOi6E_RtO_TMH3gBmK7A",
    "ksqlServiceId": "default_"
  }
}
```

You can also check the health of your KSQL server by using the
``/healthcheck`` resource:

```bash
curl -sX GET "http://localhost:8088/healthcheck" | jq '.'
```

Your output should resemble:

```json
{
  "isHealthy": true,
  "details": {
    "metastore": {
      "isHealthy": true
    },
    "kafka": {
      "isHealthy": true
    }
  }
}
```