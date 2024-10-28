---
layout: page
title: Processing Guarantees
tagline: Processing guarantees in ksqlDB
description: Learn about at-least-once, exactly-once semantics in ksqlDB
keywords: ksqldb, processing, eos, at-least-once, exactly-once
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/processing-guarantees.html';
</script>

ksqlDB supports *at-least-once* and *exactly-once* processing guarantees.

At-least-once semantics
-----------------------

Records are never lost but may be redelivered. If your stream processing
application fails, no data records are lost and fail to be processed, but some
data records may be re-read and therefore re-processed. At-least-once semantics
is enabled by default in your ksqlDB configuration, with
`processing.guarantee="at_least_once"`.

Exactly-once semantics
----------------------

Records are processed once. If a producer within a ksqlDB application
sends a duplicate record, it's written to the broker exactly once.

Exactly-once stream processing is the ability to execute a read-process-write
operation exactly one time. All of the processing happens exactly once,
including the processing and the materialized state created by the processing
job that is written back to {{ site.ak }}.

To enable exactly-once semantics, set `processing.guarantee="exactly_once_v2"` in
your ksqlDB configuration. Your {{ site.ak }} broker version must be 2.5 or later.
If you're using the {{ site.cp }} distribution of ksqlDB, you need {{ site.cp }}
version 5.5 or later.

!!! important

    Use the `exactly_once_v2` setting with care. To achieve a true exactly-once
    system, end consumers and producers must also implement exactly-once
    semantics.

For more information, see
[Processing Guarantees](https://docs.confluent.io/current/streams/concepts.html#processing-guarantees).

### Enable exactly-once semantics

Exactly-once isn't enabled by default in ksqlDB, but you can enable it on a
query-by-query basis by passing the `processing.guarantee` configuration setting
to ksqlDB.

How you pass the configuration setting to ksqlDB depends on how you
run ksqlDB Server and how you send requests to start queries.

#### ksqlDB CLI

Use the SET command to enable exactly-once for the subsequent
query:

```sql
SET 'processing.guarantee' = 'exactly_once_v2';
```

For more information, see
[Configure ksqlDB CLI](../operate-and-deploy/installation/cli-config.md).

#### REST API

Pass the config as a property along with the request:

```http
POST /query HTTP/1.1
Accept: application/vnd.ksql.v1+json
Content-Type: application/vnd.ksql.v1+json

{
"ksql": "SELECT * FROM pageviews EMIT CHANGES;",
"streamsProperties": {
    "processing.guarantee": "exactly_once_v2"
}
}
```

For more information, see
[Run a query and stream back the output](../developer-guide/ksqldb-rest-api/query-endpoint.md).

#### Default for all queries and non-interactive (headless) mode

To enable exactly-once by default for all queries, and for non-interactive
(headless) mode, set the configuration in the ksqlDB Server properties file,
which by default is located at
`${CONFLUENT_HOME}/etc/ksqldb/ksql-server.properties` in a {{ site.cp }}
deployment.

For more information, see
[ksqlDB Configuration Parameter Reference](/reference/server-configuration).

If your ksqlDB Server is deployed in a Docker container, you can enable 
exactly-once by passing in the corresponding environment variable, for example:

```bash
docker run -d \
  … 
  -e KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE=exactly_once \
  -e KSQL_BOOTSTRAP_SERVERS=localhost:9092 \
  … 
  confluentinc/ksqldb-server:{{ site.ksqldbversion }}
```

For more information, see
[Configure ksqlDB with Docker](../operate-and-deploy/installation/install-ksqldb-with-docker.md).


!!! tip

    If you use the SET command at the start of a SQL script, the setting is
    applied to all persistent queries in the script, assuming there is no
    corresponding UNSET command in the script.