---
layout: page
title: Configure KSQL CLI
tagline: Connect the KSQL CLI to KSQL Server
description: Learn how to connect the KSQL CLI to a KSQL Server cluster
keywords: ksql, cli, console, terminal
---

Configure KSQL CLI
==================

You can connect the KSQL CLI to one KSQL server per cluster.

!!! important
	There is no automatic failover of your CLI session to another KSQL
    server if the original server that the CLI is connected to becomes
    unavailable. Any persistent queries you executed will continue to run in
    the KSQL cluster.

To connect the KSQL CLI to a cluster, run this command with your KSQL
server URL specified (default is `http://localhost:8088`):

```bash
<path-to-confluent>/bin/ksql <ksql-server-URL>
```

Configuring Per-session Properties
----------------------------------

You can set the properties by using the KSQL CLI startup script argument
`/bin/ksql <server> --config-file <path/to/file>` or by using the SET
statement from within the KSQL CLI session. For more information, see
[Start the KSQL CLI](installing.md#start-the-ksql-cli).

Here are some common KSQL CLI properties that you can customize:

-   [ksql.streams.auto.offset.reset](server-config/config-reference.md#ksqlstreamsautooffsetreset)
-   [ksql.streams.cache.max.bytes.buffering](server-config/config-reference.md#ksqlstreamscachemaxbytesbuffering)
-   [ksql.streams.num.stream.threads](server-config/config-reference.md#ksqlstreamsnumstreamthreads)

Page last revised on: {{ git_revision_date }}
