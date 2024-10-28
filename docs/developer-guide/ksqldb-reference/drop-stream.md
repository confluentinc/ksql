---
layout: page
title: DROP STREAM
tagline:  ksqlDB DROP STREAM statement
description: Syntax for the DROP STREAM statement in ksqlDB
keywords: ksqlDB, stream, delete
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/drop-stream.html';
</script>

DROP STREAM
===========

Synopsis
--------

```sql
DROP STREAM [IF EXISTS] stream_name [DELETE TOPIC];
```

Description
-----------

Drops an existing stream.

If the DELETE TOPIC clause is present, the stream's source topic is
marked for deletion. If the topic format is `AVRO`, `PROTOBUF`, or `JSON_SR`, the
corresponding schema is deleted. Topic deletion is asynchronous, and actual
removal from brokers may take some time to complete.

!!! note
	DELETE TOPIC will not necessarily work if your Kafka cluster is
    configured to create topics automatically with
    `auto.create.topics.enable=true`. We recommended checking after a few
    minutes to ensure that the topic was deleted.

If the IF EXISTS clause is present, the statement doesn't fail if the
stream doesn't exist.
