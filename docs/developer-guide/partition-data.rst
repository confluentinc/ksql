.. _partition-data-to-enable-joins:

Partition Data to Enable Joins
==============================

When you use KSQL to join streaming data, you must ensure that your data
streams and tables are *co-partitioned*. Joins are performed based on record
keys, and the keys for both sides of a join must have the same number of
partitions and the same partitioning strategy. For more information on joins,
see :ref:`join-streams-and-tables`.

When your input keys are co-partitioned, records with the same key, from both
sides of the join, are delivered to the same stream task during processing.
If your input keys aren't co-partitioned, you need to re-key one of the inputs
by using the PARTITION BY clause.

Co-partitioning Requirements for Joins
**************************************

These are the requirements for data co-partitioning in a join:

* The input data for the join, on the left and right side, must have the :ref:`same keying scheme <keys-have-same-keying-scheme>`.
* The input data for the join must have the :ref:`same number of partitions <keys-have-same-number-of-partitions>`
  on both sides.
* The key for the join must be in the :ref:`same partition <keys-are-in-the-same-partition>` on both sides. 
* Both sides of the join must have the :ref:`same partitioning strategy <keys-have-partitioning-strategy>`.  

.. _keys-have-same-keying-scheme:

Same Keying Scheme
##################

The input data for the join must have the same keying scheme, which means
that the join must use the same key field on both sides.

For example, you can join a stream of user clicks that's keyed by a ``userId VARCHAR``
field with a table of user profiles that's keyed by a ``userId VARCHAR`` field. 
The join won't match if the key fields don't have the same name and type.

.. _keys-have-same-number-of-partitions:

Number of Partitions is Equal
#############################

The input data for the join must have the same number of partitions on both
sides.

KSQL verifies this part of the co-partitioning requirement by ensuring an
equal number of partitions for both sides of a join, otherwise KSQL throws
a runtime exception.

Use the ``<path-to-confluent>/bin/kafka-topics`` CLI tool
with the ``--describe`` option to see the number of partitions for the
Kafka topics that corresponding with your streams and tables.

.. _keys-are-in-the-same-partition:

Keys Are In The Same Partition
##############################

The key for the join must be in the same partition on both sides.

For example, in a stream-table join, if a ``userId`` key with the value ``alice123``
is in Partition 1 for the stream, but ``alice123`` is in Partition 2 for the table,
the join won't match, even though both sides are keyed by ``userId``.

.. _keys-have-partitioning-strategy:

Keys Have the Same Partitioning Strategy
########################################

Both sides of the join must have the same partitioning strategy, or keying
scheme. If you use the default partitioner-related settings across all
applications, you don't need to worry about the partitioning strategy.

All applications that write to the input streams must have the same partitioning
strategy, so records with the same key are delivered to same partition number.
This means that the keyspace of the input data must be distributed across
partitions in the same way on both sides of the join.

KSQL can't verify whether the partitioning strategies are the same for
the left and right inputs of a join, so you must ensure this.

Ensure Data Co-partitioning
***************************

If your inputs for a join aren't co-partitioned, you must ensure it manually
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

