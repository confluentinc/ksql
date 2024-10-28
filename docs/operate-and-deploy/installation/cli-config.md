---
layout: page
title: Configure ksqlDB CLI
tagline: Connect the ksqlDB CLI to ksqlDB Server
description: Learn how to connect the ksqlDB CLI to a ksqlDB Server cluster
keywords: ksqldb, cli, console, terminal
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/cli-config.html';
</script>

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
`/bin/ksql --config-file <path/to/file> -- <ksql-server-URL>` or by using the SET
statement from within the ksqlDB CLI session. For more information, see
[Start the ksqlDB CLI](installing.md#start-the-ksqldb-cli).

Here are some common ksqlDB CLI properties that you can customize:

-   [ksql.streams.auto.offset.reset](../../reference/server-configuration.md#ksqlstreamsautooffsetreset)
-   [ksql.streams.cache.max.bytes.buffering](../../reference/server-configuration.md#ksqlstreamscachemaxbytesbuffering)
-   [ksql.streams.num.stream.threads](../../reference/server-configuration.md#ksqlstreamsnumstreamthreads)

Connect to an Auth-enabled ksqlDB server
----------------------------------------

To connect to a ksqlDB server that requires authentication, supply your username
and password when starting the ksqlDB CLI:

```bash
/bin/ksql -u <username> -p <password> <ksql-server-URL>
```

If you don't want to supply your password as part of the command,
supply just the username, and you'll be prompted for your password
when the command executes:

```bash
/bin/ksql -u <username> <ksql-server-URL>
```

Execute Specific Statements and Quit
------------------------------------

Rather than starting an interactive ksqlDB CLI session, you can pass the
ksqlDB CLI command a file of SQL statements to execute non-interactively
using the `--file` option:

```bash
/bin/ksql --file <path/to/file> -- <ksql-server-URL>
```

You can also use the `--execute` option to pass individual statements as 
part of the command itself: 

```bash
/bin/ksql --execute <sql> -- <ksql-server-URL>
```

For example:

```bash
/bin/ksql --execute "SHOW STREAMS;" -- http://localhost:8088
```

Define Variables for Substitution in Commands
---------------------------------------------

The `--define` option allows you to specify values for variables for
use with [variable substitution](../../how-to-guides/substitute-variables.md) 
within your SQL statements. The `--define` option should be followed by a
string of the form `name=value` and may be passed any number of times.

```bash
/bin/ksql --define <variable-name-and-value> -- <ksql-server-URL>
```

For example, the following command

```bash
/bin/ksql --define stream_name=my_stream --define topic=my_topic -- http://localhost:8088
```

is equivalent to starting an interactive ksqlDB CLI session and then issuing
the following statements:

```sql
DEFINE stream_name = 'my_stream';
DEFINE topic = 'my_topic';
```

Defining variables as part of the ksqlDB CLI command is particularly useful
for variable substitution within files to be executed non-interactively.
Given the following contents of `/path/to/statements.sql`:

```sql
CREATE STREAM my_stream with (kafka_topic='${topic}', value_format='AVRO');
```

Then the following command will create a stream backed by a topic with name `my_topic`:

```bash
/bin/ksql --define topic=my_topic --file /path/to/statements.sql -- http://localhost:8088
```
