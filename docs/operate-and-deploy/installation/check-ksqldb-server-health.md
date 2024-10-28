---
layout: page
title: Check the Health of a ksqlDB Server
tagline: Make sure that ksqlDB is up and running  
description: Tips for ensuring that ksqlDB Server is well and healthy   
keywords: ksqldb, diagnostics, troubleshooting, health, jmx
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/check-ksqldb-server-health.html';
</script>

Check a ksqlDB Server from the ksqlDB CLI
-----------------------------------------

Check the streams, tables, and queries on the ksqlDB Server that you're
connected to by using the DESCRIBE EXTENDED and EXPLAIN statements
in the ksqlDB CLI.

-   Run SHOW STREAMS or SHOW TABLES, then run `DESCRIBE <stream|table> EXTENDED`.
-   Run SHOW QUERIES, then run `EXPLAIN <query-name>`.

Check a ksqlDB Server running in a native deployment
----------------------------------------------------

If you installed ksqlDB server by using a package manager, like a DEB or
RPM, or from an archive, like a TAR or ZIP file, you can check the
health of your ksqlDB Server instances by using shell commands.

### Check the ksqlDB Server process status

Use the `ps` command to check whether the ksqlDB Server process is
running:

```bash
ps -aux | grep ksql
```

Your output should resemble:

```
jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-{{ site.ksqldbversion }}/share/java/monitoring-interceptors/* ...
```

If the process status of the JVM isn't `Sl` or `Ssl`, the ksqlDB server
may be down.

Check a ksqlDB Server by using the REST API
-------------------------------------------

The ksqlDB REST API supports a "server info" request, which you access
with a URL like `http://<ksqldb-server-host>/info`. The `/info` endpoint
returns the ksqlDB Server version, the {{ site.aktm }} cluster ID, and
the service ID of the ksqlDB Server. 

Also, the ksqlDB REST API supports a basic health check endpoint at
`/healthcheck`.

!!! important
	This approach doesn't work for non-interactive, or *headless*,
    deployments of ksqlDB Server, because a headless deployment doesn't have
    a REST API server. Instead, check the [JMX metrics port](#check-the-jmx-metrics-port).

For more information, see
[Introspect server status](../../developer-guide/ksqldb-rest-api/info-endpoint.md).

Check a ksqlDB Server running in a Docker container
---------------------------------------------------

If you're running ksqlDB server in a Docker container, run the
`docker ps` or `docker-compose ps` command, and check that the status of
the `ksql-server` container is `Up`. Check the health of the process in
the container by running `docker logs <ksql-server-container-id>`.

Check the JMX metrics port
--------------------------

In addition to the previous health checks, you can query the Java Management
Extensions (JMX) port on a host that runs ksqlDB Server.

This is useful when you need to check a headless ksqlDB Server that's running
natively or in a Docker container, because headless deployments of ksqlDB Server
don't have a REST server that you can query for health. Instead, you can probe
the JMX port for liveness. A JMX probe is the most reliable way to determine
readiness of a headless deployment.

!!! note
    JMX indicates that the JVM is up and responsive. This test is similar to
    confirming if the ksqlDB process is running, but a successful response
    doesn't necessarily mean that the ksqlDB service is fully operational.
    To get better exposure, you can monitor the nodes from {{ site.c3 }} or JMX.

The following command probes the JMX port by using the Netcat utility.

```bash
nc -z <ksql-node>:1099
```

An exit code of 0 for an open port tells you that the container and ksqlDB JVM
are running. This confirmation has a level of confidence that's similar to the
REST health check.

The general responsiveness on the port should be sufficient as a high-level
health check. For a list of the available metrics you can collect, see
[JMX Metrics](/reference/metrics/).
