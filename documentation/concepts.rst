.. _ksql_concepts:

Concepts
==========

**Table of Contents**

.. contents::
  :local:


Overview of KSQL Concepts
-------------------------

The goal of this page is to cover KSQL fundamental concepts so that you understand how to effectively use KSQL to transform and enrich Kafka data.


TODO
-------------------------
* Difference between Stream and Table, KStream and KTable
* Why tables need state store and streams don't
* ROWTIME
* ROWKEY: always message key
* KSQL auto-generated topics
* Kakfa broker resource requirements
* Two intro sections: "I know SQL but new to Kafka" and "I know Kafka but new to SQL"
* Avro/SR limitations
* Kafka passes bytes, KSQL applies structure
* KSQL naming rules: `.` not allowed?, `-` needs to be escaped
* Evolve: exploration mode --> save query
* Call out `set earliest`...
* operations: how to run KSQL: standalone (bundled cli client/server), client/server split, client/server remote split (with REST API).  Diff depts that use same Kafka.  Start pool KSQL of servers (streams app) per department.
* How to run it all the time
* `partition by`: specify a non-null key, e.g., create stream s2 with (partitions = 4) as select * from orders partition by itemid;)
* To persist a query, why do we need a stream?  Why can't we write directly to just a topic?
* How users can get help (syntax guide, also built-in help functions)
