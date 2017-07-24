.. _ksql_concepts:

Concepts
==========

**Table of Contents**

.. contents::
  :local:


Overview of KSQL Concepts
-------------------------
* Two intro sections: "I know SQL but new to Kafka" and "I know Kafka but new to SQL"

The goal of this page is to cover KSQL fundamental concepts so that you understand how to effectively use KSQL to transform and enrich Kafka data.


How is KSQL different from SQL?
-------------------------------

Semantics
^^^^^^^^^
* Streaming
* STREAM, TABLE
* KSQL naming rules: `.` not allowed?, `-` needs to be escaped

KSQL dependencies on Kafka
^^^^^^^^^^^^^^^^^^^^^^^^^^
* Difference between Stream and Table, KStream and KTable
* Why tables need state store and streams don't
* Kafka passes bytes, KSQL applies structure

KSQL uses the Kafka cluster to store state. View the topics in the Kafka cluster with the ``kafka-topics`` command. You should see the topics you manually created, as well as other topics auto-created by KSQL including a ``*_commands`` topic used to story history of the KSQL commands, and ``ksql_bare_query_*`` topic that is also auto-generated for each KSQL stream you created with ``CREATE STREAM``.  For example:

.. sourcecode:: bash

   $ kafka-topics --list --zookeeper localhost:2181
   ...
   ksql_app_commands
   ksqlString
   ksql_bare_query_6739854484049497815_1500404750526-ksqlstore-changelog
   ksql_bare_query_6739854484049497815_1500404750526-ksqlstore-repartition
   ...
   <TODO: update this list based on quickstart workflow>

If you were to consume the data stored in the topic called ``*_commands``, you would see:

.. sourcecode:: bash

   $ ./bin/kafka-console-consumer --topic ksql_app_commands --bootstrap-server localhost:9092 --from-beginning --property print.key=true
   <TODO: INSERT OUTPUT>


How to Run KSQL
---------------
* Diff depts that use same Kafka.  Start pool KSQL of servers (streams app) per department.
* Standalone (bundled cli client/server)
* Client/server split, client/server remote split (with REST API)
* Kakfa broker resource requirements



Data Schemas
------------

Formatting
^^^^^^^^^^
* DELIMITED
* JSON
* AVRO
** Avro/SR limitations

Auto-generated Fields
^^^^^^^^^^^^^^^^^^^^^
* ROWTIME
* ROWKEY: always message key
* TIMESTAMP



Data Flow
---------
* Getting data into KSQL (registered topics)
* Running queries (enriching data)
* Getting data out of KSQL (writing topics)
* To persist a query, why do we need a stream?  Why can't we write directly to just a topic?
* `partition by`: specify a non-null key, e.g., create stream s2 with (partitions = 4) as select * from orders partition by itemid;)


Syntax Help
-----------
* How users can get help (syntax guide, also built-in help functions)
* Call out `set earliest`...



