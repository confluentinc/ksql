---
layout: page
title: Get the Status of a ksqlDB Server
tagline: info endpoint
description: The `/info` resource gives you the status of a ksqlDB server
keywords: ksqldb, server, status, info, terminate
---

The `/info` resource gives you information about the status of a ksqlDB
Server, which can be useful for health checks and troubleshooting. You
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

You can also check the health of your ksqlDB server by using the
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

To view non-sensitive server configurations, you can use the `/v1/configs` endpoint:

```bash
curl -sX GET "http://localhost:8088/v1/configs" | jq '.'
```

Your output should resemble:

```json
{
  "configs": {
    "ksql.query.persistent.active.limit": 20
  }
}
```

To view a specific endpoint, you can add a query:

```bash
curl -sX GET "http://localhost:8088/v1/configs?name=ksql.query.persistent.active.limit" | jq '.'
```

Currently, the only configuration that is visible is `ksql.query.persistent.active.limit`.