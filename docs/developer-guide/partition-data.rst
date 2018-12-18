.. _partition-data-to-enable-joins:

Partition Data to Enable Joins
##############################

When you use KSQL to join streaming data, you must ensure that your streams
and tables are *co-partitioned*, which means that input records on both sides
of the join have the same configuration settings for partitions.

Co-partitioning Requirements
****************************

* The input records for the join must have the :ref:`same keying scheme <keys-have-same-keying-scheme>`
* The input records must have the :ref:`same number of partitions <keys-have-same-number-of-partitions>` on both sides.
* Both sides of the join must have the :ref:`same partitioning strategy <records-have-same-partitioning-strategy>`.  

When your inputs are co-partitioned, records with the same key, from both
sides of the join, are delivered to the same stream task during processing.
If your inputs aren't co-partitioned, you need to :ref:`re-key one of the
them <ensure-co-partitioning>` by using the PARTITION BY clause.

.. _keys-have-same-keying-scheme:

Records Have the Same Keying Scheme
===================================

The input records for the join must have the same keying scheme, which means
that the join must use the same key field on both sides.

For example, you can join a stream of user clicks that's keyed by a ``VARCHAR userId``
field with a table of user profiles that's keyed by a ``VARCHAR userId`` field. 
The join won't match if the key fields don't have the same name and type.

.. _keys-have-same-number-of-partitions:

Records Have the Same Number of Partitions
==========================================

The input records for the join must have the same number of partitions on both
sides.

KSQL checks this part of the co-partitioning requirement and throws a runtime
exception if the partition count is different.

Use the ``<path-to-confluent>/bin/kafka-topics`` CLI tool
with the ``--describe`` option to see the number of partitions for the
Kafka topics that correspond with your streams and tables.

.. _records-have-same-partitioning-strategy:

Records Have the Same Partitioning Strategy
===========================================

Records on both sides of the join must have the same partitioning strategy.
If you use the default partitioner settings across all applications, you don't
need to worry about the partitioning strategy.

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

.. _ensure-co-partitioning:

Ensure Data Co-partitioning
***************************

If your join inputs aren't co-partitioned, you must ensure it manually
by re-keying the data on one side of the join.

For example, in a stream-table join, if a stream of user clicks is keyed by
``pageId``, but a table of user profiles is keyed by ``userId``, one of the
two inputs must be re-keyed (re-partitioned). Which of the two should be re-keyed
depends on the situation.

If the stream has very high volume, you may not want to re-key it,
because this would duplicate a large data source. Instead, you may prefer to
re-key the smaller table.

To enforce co-partitioning, use the PARTITION BY clause.

For example, if you need to re-partition a stream to be keyed by a ``product_id`` 
field, and keys need to be distributed over 6 partitions to make a join work,
use the following KSQL statement:

.. code:: sql

   CREATE STREAM products_rekeyed WITH (PARTITIONS=6) AS SELECT * FROM products PARTITION BY product_id;

For more information, see `Inspecting and Changing Topic Keys <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/inspecting-changing-topic-keys>`__ 
in the `Stream Processing Cookbook <https://www.confluent.io/product/ksql/stream-processing-cookbook>`__.

