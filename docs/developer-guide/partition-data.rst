.. _partition-data-to-enable-joins:

Partition Data to Enable Joins
##############################

When you use KSQL to join streaming data, you must ensure that your streams
and tables are *co-partitioned*, which means that input records on both sides
of the join have the same configuration settings for partitions.

Co-partitioning Requirements
****************************

* The input records for the join must have the :ref:`same key schema <keys-have-same-key-schema>`
* The input records must have the :ref:`same number of partitions <keys-have-same-number-of-partitions>` on both sides.
* Both sides of the join must have the :ref:`same partitioning strategy <records-have-same-partitioning-strategy>`.  

When your inputs are co-partitioned, records with the same key, from both
sides of the join, are delivered to the same stream task during processing.

.. _keys-have-same-key-schema:

Records have the Same Key Schema
================================

For a join to work the keys from both sides must have the same serialized binary data.

For example, you can join a stream of user clicks that's keyed on a ``VARCHAR`` user id with a table
of user's profiles that's also keyed on a ``VARCHAR`` user id. Records with the exact same user id
on both sides will be joined.

Tables created on top of existing Kafka topics, for example those created with a ``CREATE TABLE``
statement, will be keyed on the data held in the key of the records in the
Kafka topic.  KSQL presents this data in the ``ROWKEY`` column, and expects the data to be a ``VARCHAR``.

Tables created inside KSQL from other sources, for example those created with a ``CREATE TABLE AS SELECT``
statement, will copy the key from their source(s) unless there is an explicit ``GROUP BY`` or ``PARTITION BY``
clause, which can changed what the table is keyed on.

KSQL will automatically repartition a stream of data before joining, if required. In the example below
KSQL is used to join a stream of clicks with a table of users.  The ``clicks`` stream is not keyed on anything
initially. The ``users`` table keyed on ``userId``.  Because the stream is being joined on a column other than
it's key field, i.e. ``ROWKEY``, KSQL will automatically repartition the stream under the hood.

.. code:: sql

   -- clicks stream, with an unknown key.
   CREATE STREAM clicks (userId STRING, url STRING) WITH(kafka_topic='clickstream', value_format='json');

   -- users table, where the records have the userId in the record's key:
   CREATE TABLE  users  (userId STRING, fullName STRING) WITH(kafka_topic='users', value_format='json', key='userId');

   -- join will automatically repartition clicks stream:
   SELECT clicks.url, users.fullName FROM clicks JOIN users ON clicks.userId = users.userId;

.. note::

   While KSQL will automatically repartition a stream should a join require it, KSQL will reject any join
   any table's column that is not the key. This is because KSQL does not support joins on foreign keys
   and repartitioning a table would corrupt the data.

If you are using the same sources in more than one join that requires the data to be repartitioned you
may choose to repartition manually to avoid KSQL repartitioning multiple times.

To repartition a stream, use the PARTITION BY clause.

For example, if you need to re-partition a stream to be keyed by a ``product_id``
field, and keys need to be distributed over 6 partitions to make a join work,
use the following KSQL statement:

.. code:: sql

   CREATE STREAM products_rekeyed WITH (PARTITIONS=6) AS SELECT * FROM products PARTITION BY product_id;

For more information, see `Inspecting and Changing Topic Keys <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/inspecting-changing-topic-keys>`__
in the `Stream Processing Cookbook <https://www.confluent.io/product/ksql/stream-processing-cookbook>`__.

.. _keys-have-same-number-of-partitions:

Records Have the Same Number of Partitions
==========================================

The input records for the join must have the same number of partitions on both
sides.

KSQL checks this part of the co-partitioning requirement and rejects any join where the partition counts differ.

Use the ``DESCRIBE EXTENDED <source name>`` command in the CLI to determine the Kafka topic under a source,
and use the ``SHOW TOPICS;`` command in the CLI to list out topics and their partition counts.

If the sides of the join have different partition counts then you may wish to look at changing the partition counts
of the source topics, or repartition one side to match the partition count of the other.

The following example will create a repartitioned stream, maintaining the existing key, and with the
specified number of partitions:

.. code:: sql

   CREATE STREAM products_rekeyed WITH (PARTITIONS=6) AS SELECT * FROM products PARTITION BY ROWKEY;

.. _records-have-same-partitioning-strategy:

Records Have the Same Partitioning Strategy
===========================================

Records on both sides of the join must have the same partitioning strategy.
If you use the default partitioner settings across all applications, and your producers aren't
specifying an explicit partition, you don't need to worry about the partitioning strategy.

But if the producer applications for your records have custom partitioners
specified in `configuration <http://kafka.apache.org/documentation/#producerconfigs>`__,
the same custom partitioner logic must be used for records on both sides of the join.
The applications that write to the join inputs must have the same partitioning
strategy, so that records with the same key are delivered to same partition number.

This means that the input records must be in the same partition on both sides
of the join. For example, in a stream-table join, if a ``userId`` key with the
value ``alice123`` is in Partition 1 for the stream, but ``alice123`` is in
Partition 2 for the table, the join won't match, even though both sides are
keyed by ``userId``.

KSQL can't verify whether the partitioning strategies are the same for
both join inputs, so you must ensure this.

The `DefaultPartitioner class <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java>`__
implements the following partitioning strategy:

* If the producer specifies a partition in the record, use it.
* If the producer specifies a key instead of a partition, choose a partition
  based on a hash of the key.
* If the producer doesn't specify a partition or a key, choose a partition in
  a round-robin fashion.

Custom partitioner classes implement the `Partitioner interface <https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/Partitioner.html>`__ 
and are assigned in the producer configuration property, ``partitioner.class``.

For example implementations of a custom partitioner, see
`Built for realtime: Big data messaging with Apache Kafka, Part 2 <https://www.javaworld.com/article/3066873/big-data/big-data-messaging-with-kafka-part-2.html>`__
and `Apache Kafka Foundation Course - Custom Partitioner <https://www.learningjournal.guru/courses/kafka/kafka-foundation-training/custom-partitioner/>`__.

