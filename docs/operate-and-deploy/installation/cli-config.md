---
layout: page
title: Configure ksqlDB CLI
tagline: Connect the ksqlDB CLI to ksqlDB Server
description: Learn how to connect the ksqlDB CLI to a ksqlDB Server cluster
keywords: ksqldb, cli, console, terminal
---

You can connect the ksqlDB CLI to one ksqlDB server per cluster.

!!! important
	There is no automatic failover of your CLI session to another ksqlDB
    Server if the original server that the CLI is connected to becomes
    unavailable. Any persistent queries you executed will continue to run in
    the ksqlDB cluster.

To connect the ksqlDB CLI to a cluster, run the following command with your
ksqlDB server URL specified (default is `http://localhost:8088`):

```bash
<path-to-confluent>/bin/ksql <ksql-server-URL>
```

Configure Per-session Properties
--------------------------------

You can set the properties by using the ksqlDB CLI startup script argument
`/bin/ksql <server> --config-file <path/to/file>` or by using the SET
statement from within the ksqlDB CLI session. For more information, see
[Start the ksqlDB CLI](installing.md#start-the-ksqldb-cli).

Here are some common ksqlDB CLI properties that you can customize:

-   [ksql.streams.auto.offset.reset](/reference/server-configuration#ksqlstreamsautooffsetreset)
-   [ksql.streams.cache.max.bytes.buffering](/reference/server-configuration#ksqlstreamscachemaxbytesbuffering)
-   [ksql.streams.num.stream.threads](/reference/server-configuration#ksqlstreamsnumstreamthreads)

