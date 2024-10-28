---
layout: page
title: Tables 
tagline: The table concept in ksqlDB
description: Summary of the table concept in ksqlDB
keywords: table, partition, key
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/concepts/tables.html';
</script>

A table is a mutable, partitioned collection that models change over time. In
contrast with a stream, which represents a historical sequence of events, a
table represents what is true as of "now". For example, you might use a table
to model the locations where someone has lived as a stream: first Miami, then
New York, then London, and so forth.

Tables work by leveraging the keys of each row. If a sequence of rows shares a
key, the last row for a given key represents the most up-to-date information
for that key's identity. A background process periodically runs and deletes all
but the newest rows for each key.