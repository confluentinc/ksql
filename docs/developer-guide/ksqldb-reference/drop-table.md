---
layout: page
title: DROP TABLE
tagline:  ksqlDB DROP TABLE statement
description: Syntax for the DROP TABLE statement in ksqlDB
keywords: ksqlDB, table, delete
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/drop-table.html';
</script>

DROP TABLE
==========

Synopsis
--------

```sql
DROP TABLE [IF EXISTS] table_name [DELETE TOPIC];
```

Description
-----------

Drops an existing table.

If the DELETE TOPIC clause is present, the table's source topic is
marked for deletion. If the topic format is `AVRO` or `PROTOBUF`, the
corresponding schema is deleted in the schema registry. Topic deletion is
asynchronous, and actual removal from brokers may take some time to
complete.

!!! note
	DELETE TOPIC will not necessarily work if your {{ site.ak }} cluster is
    configured to create topics automatically with
    `auto.create.topics.enable=true`. We recommended checking after a few
    minutes to ensure that the topic was deleted.

If the IF EXISTS clause is present, the statement doesn't fail if the
table doesn't exist.

The DROP TABLE statement and the DELETE TOPIC clause are not atomic, because
the schema subject is soft-deleted. The soft delete happens before the table is
dropped, and the table may not appear to be dropped. Subsequent attempts to
drop the table fail, because the subject is already soft-deleted.

To work around this situation, hard-delete the subject, then re-run the DROP
TABLE statement.
