---
layout: page
title: Check the Health of a ksqlDB Server
tagline: Make sure that ksqlDB is up and running  
description: Tips for ensuring that ksqlDB Server is well and healthy   
keywords: ksqldb, diagnostics, troubleshooting
---

Check a ksqlDB Server Running in a Native Deployment
----------------------------------------------------

If you installed ksqlDB server by using a package manager, like a DEB or
RPM, or from an archive, like a TAR or ZIP file, you can check the
health of your ksqlDB Server instances by using shell commands.

Use the `ps` command to check whether the ksqlDB Server process is
running:

```bash
ps -aux | grep ksql
```

Your output should resemble:

```
jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-{{ site.release }}/share/java/monitoring-interceptors/* ...
```

If the process status of the JVM isn't `Sl` or `Ssl`, the ksqlDB server
may be down.

Check runtime stats for the ksqlDB server that you're connected to.

-   Run `ksql-print-metrics` on a server host. The tool connects to
    a ksqlDB Server that's running on `localhost` and collects JMX
    metrics from the server process. Metrics include the number of
    messages, the total throughput, the throughput distribution, and
    the error rate. For more information, see
    [Monitoring and Metrics](../operations.md#monitoring-and-metrics)
-   In the ksqlDB CLI or in {{ site.c3 }}, run SHOW STREAMS or SHOW
    TABLES, then run DESCRIBE EXTENDED <stream|table>.
-   In the ksqlDB CLI or in {{ site.c3 }}, run SHOW QUERIES, then run
    EXPLAIN <query>.

Check a ksqlDB Server by using the REST API
-------------------------------------------

The ksqlDB REST API supports a "server info" request, which you access
with a URL like `http://<ksqldb-server-url>/info`. The `/info` endpoint
returns the ksqlDB Server version, the {{ site.aktm }} cluster ID, and
the service ID of the ksqlDB Server. For more information, see
[ksqlDB REST API Reference](../developer-guide/api.md).

```bash
curl -sX GET "http://localhost:8088/info"
```

Your output should resemble:

```json
{
    "KsqlServerInfo":{
        "version":"{{ site.release }}",
        "kafkaClusterId":"X5ZV2fjQR1u4zQDLlw62PQ",
        "ksqlServiceId":"default_"
    }
}
```

!!! note
	This approach doesn't work for non-interactive, or *headless*,
    deployments of ksqlDB Server, because a headless deployment doesn't have
    a REST API server.

ksqlDB Server Running in a Docker Container
-------------------------------------------

If you're running ksqlDB server in a Docker container, run the
`docker ps` or `docker-compose ps` command, and check that the status of
the `ksql-server` container is `Up`. Check the health of the process in
the container by running `docker logs <ksql-server-container-id>`.

Page last revised on: {{ git_revision_date }}
