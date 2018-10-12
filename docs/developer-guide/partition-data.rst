.. _partition-data-to-enable-joins:

Partition Data to Enable Joins
==============================

When you use KSQL to join streaming data, you must ensure that your data
streams and tables are *co-paritioned*. Joins are performed based on record
keys, and the keys for both sides of a join must have the same number of
partitions and the same partitioning strategy.

When your input keys are co-partitioned, records with the same key, from both
sides of the join, are delivered to the same stream task during processing. [TBD: diagram]

The requirements for data co-partitioning are:

* The input data for the join, on the left and right side, must have the same
  number of partitions.
* All applications that write to the input streams must have the same partitioning
  strategy, so records with the same key are delivered to same partition number.
  This means that the keyspace of the input data must be distributed across
  partitions in the same way. If you use the default partitioner-related settings
  across all applications, you don't need to worry about the partitioning strategy.

KSQL verifies part of the co-partitioning requirement by ensuring an equal 
number of partitions for both sides of a join, otherwise KSQL throws a runtime
exception. KSQL can't verify whether the partitioning strategy matches between
the input data of a join, so you must ensure this.

Ensure Data Co-partitioning
***************************

If your inputs for a join aren't co-partitioned currently, you must ensure it
manually. The following steps show how to co-partition your data.

#. Identify the input stream or table in the join that has the smaller number
   of partitions. Call this stream or table ``SMALLER``, and the other side of
   the join ``LARGER``. Use the CLI tool <path-to-confluent>/bin/kafka-topics
   with the ``--describe`` option to find the number of partitions for the
   corresponding  Kafka topic.

#. Pre-create a new Kafka topic for “SMALLER” that has the same number of partitions
   as ``LARGER``. Call this new topic ``repartitioned-topic-for-smaller``. Use
   the CLI tool <path-to-confluent>/bin/kafka-topics with the ``--create`` option for this.

TBD: Replace KStreams code with KSQL equivalent:

#. Within your application, re-write the data of “SMALLER” into the new Kafka topic.
You must ensure that, when writing the data with to or through, the same partitioner
is used as for “LARGER”.

If “SMALLER” is a KStream: KStream#to("repartitioned-topic-for-smaller").
If “SMALLER” is a KTable: KTable#to("repartitioned-topic-for-smaller").
Within your application, re-read the data in “repartitioned-topic-for-smaller” into a new KStream/KTable.

If “SMALLER” is a KStream: StreamsBuilder#stream("repartitioned-topic-for-smaller").
If “SMALLER” is a KTable: StreamsBuilder#table("repartitioned-topic-for-smaller").
Within your application, perform the join between “LARGER” and the new stream/table.


