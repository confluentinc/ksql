---
layout: page
title: Get the Status of a ksqlDB Server
tagline: info endpoint
description: The `/info` resource gives you the status of a ksqlDB server
keywords: ksqldb, server, status, info, terminate
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-rest-api/info-endpoint.html';
</script>

The `/info` resource gives you information about the status of a ksqlDB
Server, which can be useful for health checks and troubleshooting. You
can use the `curl` command to query the `/info` endpoint:

```bash
curl --http1.1 -sX GET "http://localhost:8088/info" | jq '.'
```

Your output should resemble:

```json
{
  "KsqlServerInfo": {
    "version": "{{ site.ksqldbversion }}",
    "kafkaClusterId": "j3tOi6E_RtO_TMH3gBmK7A",
    "ksqlServiceId": "default_"
  }
}
```

You can also check the health of your ksqlDB server by using the
``/healthcheck`` resource:

```bash
curl --http1.1 -sX GET "http://localhost:8088/healthcheck" | jq '.'
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

