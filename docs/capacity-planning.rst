.. _ksql_capacity_planning:

======================
KSQL Capacity Planning
======================

KSQL is a simple and powerful tool for building streaming applications on top of |ak-tm|. This guide helps you plan for provisioning your KSQL deployment and answers these questions:

- What server specification should I use to run KSQL?
- Approximately how many KSQL server nodes do I need?
- Do I need to provision additional Kafka brokers to support KSQL?
- What kind of deployment mode makes sense for me?
- How do I know whether my KSQL queries are handling the incoming message rate, and how can I tune KSQL if it’s falling behind?

.. tip:: Because the underlying implementation of KSQL uses the :ref:`Kafka Streams API
         <streams_developer-guide_dsl>`  for query
         processing, the details covered in the Streams documentation apply to KSQL as well. The :ref:`Kafka
         Streams capacity planning guide <streams_sizing>`
         is another useful resource for KSQL capacity planning.

Approach To Sizing
==================

This document will provide you with a rough estimate of the computing resources required to run your KSQL queries. There are many factors that will determine your actual required resources, and the best practice is to stage and test your queries against realistic loads first, and then adjust your planned provisioning accordingly.

General Guidelines
==================

KSQL
----

**CPU**: KSQL consumes CPU to serialize and deserialize messages into the declared stream and table schemas, and then process each message as required by the query. At least 4 cores are recommended.

**Disk**: KSQL uses local disk to persist temporary state for aggregations and joins. An SSD and at least 100 GB is recommended.

**Memory**: KSQL memory usage is dominated by on-heap message processing and off-heap state for aggregations (e.g., SUM, COUNT, TOPKDISTINCT) and joins. Usage for message processing scales with message throughput, while aggregation and join state is a function of topic partition count, key space size, and windowing. A good starting point here is 32 GB.

**Network**: KSQL relies heavily on Kafka, so fast and reliable network is important for optimal throughput. A 1 Gbit NIC is a good starting point.

General guidelines for a basic KSQL server are:

- 4 cores
- 32 GB RAM
- 100 GB SSD
- 1 Gbit network

Kafka
-----

KSQL consumes resources on your Kafka cluster.

Generated Topics
++++++++++++++++

KSQL creates the following types of topics on your Kafka cluster:

**Output Topics**

Every query started by a ``CREATE STREAM AS SELECT`` or ``CREATE TABLE AS SELECT`` statement writes its results to an output topic. The created topic is configured with the following properties:

- Name: By default KSQL creates the output topic with the same name as the stream or table created by the statement. You can specify a custom name in the ``KAFKA_TOPIC`` property of the statement's ``WITH`` clause.
- Partitions: By default KSQL creates an output topic with 4 partitions. You can specify a custom partition count in the ``PARTITIONS`` property of the statement's ``WITH`` clause.
- Replication Factor: By default KSQL creates the output topic with a replication factor of 1. You can specify a custom replication factor in the ``REPLICAS`` property of the statement's ``WITH`` clause.

**Internal Topics for Repartitioning**

Some queries require that the input stream be repartitioned so that all messages being aggregated or joined together reside in the same partition. Repartitioning means that an intermediate topic is created and every record is produced and consumed to and from that topic with a key that ensures the locality requirements of the query are satisfied. The intermediate topic is named with the suffix *repartition*. The repartition topic has the same number of partitions and replicas as the input stream for aggregations, or the input table for joins.

.. tip::
    To determine if your query needs a repartition, you can use the ``DESCRIBE EXTENDED`` and ``EXPLAIN`` statements.
    For example, consider the following table created from the quickstart:

    .. code:: sql

        CREATE TABLE pageviews_by_page AS SELECT pageid, COUNT(*) FROM pageviews_original GROUP BY pageid;
        DESCRIBE EXTENDED pageviews_by_page;
        
    Your output should resemble:

    ::

        ...
        Queries that write into this TABLE
        -----------------------------------
        id:CTAS_PAGEVIEWS_BY_PAGE - CREATE TABLE pageviews_by_page AS SELECT pageid, COUNT(*) FROM pageviews_original GROUP BY pageid;

        For query topology and execution plan please run: EXPLAIN <QueryId>

    The DESCRIBE EXTENDED output includes the Query ID for the query populating the table. You can run EXPLAIN against the query to print the underlying streams topology:

    .. code:: text

        EXPLAIN CTAS_PAGEVIEWS_BY_PAGE;

    Your output should resemble:

    ::

        Type                 : QUERY
        SQL                  : CREATE TABLE pageviews_by_page AS SELECT pageid, COUNT(*) FROM pageviews_original GROUP BY pageid;

        Execution plan
        --------------
        > [ PROJECT ] Schema: [PAGEID : STRING , KSQL_COL_1 : INT64].
               > [ AGGREGATE ] Schema: [PAGEVIEWS_ORIGINAL.PAGEID : STRING , PAGEVIEWS_ORIGINAL.ROWTIME : INT64 , KSQL_AGG_VARIABLE_0 : INT64].
                       > [ PROJECT ] Schema: [PAGEVIEWS_ORIGINAL.PAGEID : STRING , PAGEVIEWS_ORIGINAL.ROWTIME : INT64].
                               > [ SOURCE ] Schema: [PAGEVIEWS_ORIGINAL.ROWTIME : INT64 , PAGEVIEWS_ORIGINAL.ROWKEY : STRING , PAGEVIEWS_ORIGINAL.VIEWTIME : INT64 , PAGEVIEWS_ORIGINAL.USERID : STRING , PAGEVIEWS_ORIGINAL.PAGEID : STRING].

        Processing topology
        -------------------
        Topologies:
           Sub-topology: 0
            Source: KSTREAM-SOURCE-0000000000 (topics: [pageviews])
              --> KSTREAM-MAPVALUES-0000000001
            Processor: KSTREAM-MAPVALUES-0000000001 (stores: [])
              --> KSTREAM-TRANSFORMVALUES-0000000002
              <-- KSTREAM-SOURCE-0000000000
            Processor: KSTREAM-TRANSFORMVALUES-0000000002 (stores: [])
              --> KSTREAM-MAPVALUES-0000000003
              <-- KSTREAM-MAPVALUES-0000000001
            Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])
              --> KSTREAM-FILTER-0000000004
              <-- KSTREAM-TRANSFORMVALUES-0000000002
            Processor: KSTREAM-FILTER-0000000004 (stores: [])
              --> KSTREAM-KEY-SELECT-0000000005
              <-- KSTREAM-MAPVALUES-0000000003
            Processor: KSTREAM-KEY-SELECT-0000000005 (stores: [])
              --> KSTREAM-FILTER-0000000009
              <-- KSTREAM-FILTER-0000000004
            Processor: KSTREAM-FILTER-0000000009 (stores: [])
              --> KSTREAM-SINK-0000000008
              <-- KSTREAM-KEY-SELECT-0000000005
            Sink: KSTREAM-SINK-0000000008 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)
              <-- KSTREAM-FILTER-0000000009
        
          Sub-topology: 1
            Source: KSTREAM-SOURCE-0000000010 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])
              --> KSTREAM-AGGREGATE-0000000007
            Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])
              --> KTABLE-MAPVALUES-0000000011
              <-- KSTREAM-SOURCE-0000000010
            Processor: KTABLE-MAPVALUES-0000000011 (stores: [])
              --> KTABLE-TOSTREAM-0000000012
              <-- KSTREAM-AGGREGATE-0000000007
            Processor: KTABLE-TOSTREAM-0000000012 (stores: [])
              --> KSTREAM-MAPVALUES-0000000013
              <-- KTABLE-MAPVALUES-0000000011
            Processor: KSTREAM-MAPVALUES-0000000013 (stores: [])
              --> KSTREAM-SINK-0000000014
              <-- KTABLE-TOSTREAM-0000000012
            Sink: KSTREAM-SINK-0000000014 (topic: PAGEVIEWS_BY_PAGE)
              <-- KSTREAM-MAPVALUES-0000000013

    Observe that there are 2 sub-topologies. This means that the stream is being re-partitioned.

**State Store Changelog Topics**

KSQL uses an embedded storage engine to manage state locally for operations such as aggregations. For fault-tolerance reasons it also persists the state for aggregations (e.g, SUM, COUNT, TOPKDISTINCT) in a compacted changelog topic. The changelog topic has the same number of partitions as the input stream. It defaults to a single replica, but this can be explicitly set via the ``ksql.streams.replication.factor`` property.

The amount of data stored in the changelog topic depends on the number of keys, key size, aggregate size, and whether the aggregation is windowed and if so, what the window retention time is. 

For un-windowed aggregations the total size should be roughly the (key size + aggregate size) multiplied by the number of keys.

For windowed aggregates the size is determined by the number of outstanding windows multiplied by the size of each window. The number of outstanding windows  is bound by the window retention time. The size of each window depends on message throughput, key space size and the average key size. If you have a large key space then each window’s size will likely be a multiple of the throughput, window size, and average key size. If your key space is small then the window’s size is bound by the number of keys multiplied by the average key size.

**KSQL Command Topics**

When run in interactive configuration, a KSQL cluster creates an internal topic (whose name is derived from the ``ksql.service.id`` setting) to persist the log of queries to run across all the servers in this KSQL cluster. These special-purpose topics for KSQL are called command topics.  Command topics have a single partition and default to a replication factor of 1.

.. note::

   Headless mode deployments don't have a command topic.

Consumption and Production
++++++++++++++++++++++++++

You might need to provision additional Kafka brokers to accommodate KSQL production and consumption to and from your Kafka cluster.

Minimally, each query consumes each record from an input Kafka topic and produces records to an output Kafka topic.

Some queries require that the input stream be repartitioned so that all messages being aggregated or joined together reside in the same partition. Each repartition produces and consumes every record.

Finally, stateful queries such as aggregations and joins produce records to a changelog topic for their respective state stores. 

Important Sizing Factors
========================

This section describes the important factors to consider when scoping out your KSQL deployment.

**Throughput**: In general, higher throughput requires more resources.

**Query Types**: Your realized throughput will largely be a function of the type of queries you run. You can think of KSQL queries as falling into these categories:

- Project/Filter, e.g. ``SELECT <columns> FROM <table/stream> WHERE <condition>``
- Joins
- Aggregations, e.g. ``SUM, COUNT, TOPK, TOPKDISTINCT``

A project/filter query reads records from an input stream or table, may filter the records according to some predicate, and performs stateless transformations on the columns before writing out records to a sink stream or table. Project/filter queries require the fewest resources. For a single project/filter query running on an instance provisioned as recommended above you can expect to realize from ~40 MB/second up to the rate supported by your network. The throughput largely depends on the average message size and complexity. Processing small messages with many columns is CPU intensive and will saturate your CPU. Processing large messages with fewer columns requires less CPU and KSQL will start saturating the network for such workloads.

Stream-table joins read and write to Kafka Streams state stores and require around twice the CPU of project/filter. Though Kafka Streams state stores are stored on disk, it is recommended that you provision sufficient memory to keep the working set memory-resident to avoid expensive disk i/os. So expect around half the throughput and expect to provision higher-memory instances.

Aggregations read from and may write to a state store for every record. They consume around twice the CPU of joins. The CPU required increases if the aggregation uses a window as the state store must be updated for every window.

**Number of Queries**: The available resources on a server are shared across all queries. So expect that the processing throughput per server will decrease proportionally with the number of queries it is executing (see the notes on vertically and horizontally scaling a KSQL cluster in this document to add more processing capacity in such situations) . Furthermore, KSQL queries run as Kafka Streams applications. Each query starts its own Kafka Streams worker threads, and uses its own consumers and producers. This adds a little bit of CPU overhead per query. You should avoid running a large number of queries on one KSQL cluster. Instead, use interactive mode to play with your data and develop sets of queries that function together. Then, run these in their own headless cluster. Check out the :ref:`Recommendations and Best Practices <ksql_sizing_best>` section for more details.

**Data Schema**: KSQL handles mapping serialized Kafka records to columns in a stream or table’s schema. In general, more complex schemas with a higher ratio of columns to bytes of data require more CPU to process.

**Number of Partitions**: Kafka Streams creates one RocksDB state store instance for aggregations and joins for every topic partition processed by a given KSQL server. Each RocksDB state store instance has a memory overhead of 50 MB for its cache plus the data actually stored.

**Key Space**: For aggregations and joins, Kafka Streams/RocksDB will try and keep the working set of a state store in memory to avoid I/O operations. If there are many keys then this will require more memory. It also makes reads and writes to the state store more expensive. Note that the size of the data in a state store is not limited by memory (RAM) but only by available disk space on a KSQL server.

.. _ksql_sizing_best:

Recommendations and Best Practices
==================================

Interactive KSQL Servers vs Non-Interactive (“Headless”) Servers 
----------------------------------------------------------------

By default, KSQL servers are configured for interactive use, which means you can use the KSQL CLI to interact with a KSQL cluster in order to, for example, execute new queries. Interactive KSQL usage allows for easy and quick iterative development and testing of your KSQL queries via the KSQL CLI.

You can also :ref:`configure the servers for headless, non-interactive operation <restrict-ksql-interactive>`, where servers collaboratively run only a predefined list of queries. The result is essentially an elastic, fault-tolerant, and distributed stream processing application that communicates to the outside world by reading from and writing to Kafka topics.  Sizing, deploying, and managing in this scenario is similar to a :ref:`Kafka Streams application <kafka_streams>`. You should integrate KSQL deployments with your own CI/CD pipeline, for example, to version-control the .sql file.

Here are some guidelines for choosing between the configuration types:

- For production deployments, headless, non-interactive KSQL clusters are recommended. This configuration provides the best isolation and, unlike interactive KSQL clusters, minimizes the likelihood of operator error and human mistakes.
- For exploring and experimenting with your data, interactive KSQL clusters are recommended. With this method you can quickly create queries for your use case that will function as a streaming “application” to produce meaningful results. You can then run this “application” with headless, non-interactive KSQL clusters in production.
- For interactive KSQL usage, you should deploy an interactive KSQL cluster per project or per team instead of a single, large KSQL cluster for your organization.

.. _ksql-capacity-planning-scaling:

Scaling KSQL
------------

You can scale KSQL by adding more capacity per server (i.e., vertically) or by adding more servers (i.e., horizontally). You can elastically scale KSQL clusters during live operations without loss of data. For example, you can add and remove KSQL servers to increase or decrease processing capacity. When scaling vertically, configure servers with a larger number of stream threads. For more information, see :ref:`ksql-streams-num-streams-threads`. If you are scaling past eight cores, it’s generally recommended to scale horizontally by adding servers.

Similar to Kafka Streams, KSQL throughput scales well as resources are added, if your Kafka topics have enough partitions to increase parallelism. For example, if your input topic has five partitions, the maximum parallelism is also five; a maximum of five cores/threads would execute a query on this topic in parallel. If you want to increase the maximum level of parallelism, you must increase the number of partitions that are being processed by using one of these methods:

- Re-partition your input data into a new stream with the ``CREATE STREAM AS SELECT`` statement and then write subsequent
  queries against the repartitioned stream. Also, if you want to save storage space in your Kafka cluster, consider
  lowering the data retention configuration for that underlying stream topic.
- Increase the number of partitions in the input topic.

To scale KSQL horizontally, run additional KSQL servers with the same ``ksql.service.id``.

You can add KSQL Server instances continuously to scale load horizontally, as
long as there are more partitions than consumers.

How to Know When to Scale
+++++++++++++++++++++++++

If KSQL cannot keep up with the production rate of your Kafka topics, it will start to fall behind in processing the incoming data. Consumer lag is the Kafka terminology for describing how much a Kafka consumer including KSQL has fallen behind. It’s important to monitor consumer lag on your topics and add resources if you observe that the lag is growing. :ref:`control_center` is the recommended tool for monitoring. You can also check out :ref:`Kafka documentation <kafka_monitoring>` for details on metrics exposed by Kafka that can be used to monitor lag.

Mixed Workloads
+++++++++++++++

Your workload may involve multiple queries, perhaps with some feeding data into others in a streaming pipeline. For example, a project/filter to transform some data that is then aggregated. Monitoring consumer lag of each query’s input topic is especially important for such workloads. KSQL currently does not have a mechanism to guarantee resource utilization fairness between queries. So a faster query like a project/filter may “starve” a more expensive query like a windowed aggregate if the production rate into the source topics is high. If this happens you will observe growing lag on the source topic for the more expensive queries and very low throughput to their sink topics.

You can fix this situation by using either of these methods:

- Tune the cheaper queries to consume less CPU by decreasing ``kafka.streams.num.threads`` for that query.
- Add resources to reduce the per-CPU usage of the cheaper queries, which in turn will increase the throughput for the
  more expensive queries.

Examples
========

This section provides sizing scenarios with examples of how to think about sizing. These examples analyze a stream of
pageview events.

.. note:: The environment and numbers in this section are hypothetical and only meant for illustration purposes. You should perform your own benchmarking and testing to match your use cases and environments.

The examples assume the following DDL statements to declare the schema for the input data:

.. code:: sql

    CREATE STREAM pageviews_original
        (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, client_ip INT, url VARCHAR, duration BIGINT, from_url VARCHAR, analytics VARCHAR)
        WITH (kafka_topic='pageviews', value_format=’JSON’, KEY=’userid’);

    CREATE TABLE users (registertime BIGINT, gender VARCHAR, city INT, country INT, userid VARCHAR, email VARCHAR)
        WITH (kafka_topic='users', value_format='JSON', key = 'userid');

These assumptions are also made:

- The production rate into the ``pageviews`` topic is 50 MB/s.
- The messages in ``pageviews`` average 256 bytes.
- The ``pageviews`` topic has 64 partitions.
- The messages are in JSON format. Serialization to JSON adds some space overhead. You can assume an extra 25% to account for this.

Scenario 1: Project/Filter Only (Stateless Queries)
---------------------------------------------------

In this scenario my application is a single project/filter query that tries to capture meaningful pageviews by filtering
out all the views that lasted less than 10 seconds:

.. code:: sql

    CREATE STREAM pageviews_meaningful
        WITH (PARTITIONS=64) AS
        SELECT *
        FROM pageviews_original
        WHERE duration > 10;

KSQL
++++

The example pageviews messages are under 256 bytes. For smaller messages, in this hypothetical environment, you can assume
each 4-core KSQL server is CPU bound at around 50 MB/s. This throughput can be managed with a single KSQL server. For
increased fault-tolerance, you can run a second server.

Project/Filter is stateless, and therefore does not have to account for state store memory. 8 GB are recommended for
the Java heap space for record processing.

KSQL uses the network to consume records from the Kafka input topic and produce records to the output topic. In this example
query 50 MB/s are received. If you assume that 90% of the page views are meaningful, then you would produce 45 MB/s as output.

Kafka
+++++

On the Kafka side you would need to provision for the additional production and consumption bandwidth as calculated above.
Additionally, you would need to account for the output topic itself, which would add 64 partitions to the Kafka cluster.

Scenario 2: Large Messages
--------------------------

In this example the same query as scenario 1 is performed, but each message is 8 KB. For larger messages, each KSQL node is usually
network bound, instead of CPU bound. One node with a 1 Gb/s should be able to manage the original 50 MB/s (400 Mb/s) of
throughput coming into the ``pageviews_original`` topic. You can assume the production throughput is larger at 256 MB/s.
A 1 Gb/s full-duplex NIC can handle 1 Gb/s, or 128 MB/s in each direction. You can estimate 2-3 KSQL nodes are required
to manage this load.

Scenario 3: More Advanced Usage
-------------------------------

In this example, the messages are 256 bytes and you want to enrich ``pageviews_meaningful`` with information about the user,
and then count up views by city:

.. code:: sql

    CREATE STREAM pageviews_meaningful_with_user_info
        WITH (PARTITIONS=64) AS
        SELECT pv.viewtime, pv.userid, pv.pageid, pv.client_ip, pv.url, pv.duration, pv.from_url, u.city, u.country, u.gender, u.email
        FROM pageviews_meaningful pv LEFT JOIN users u ON pv.userid = u.userid;

    CREATE TABLE pageview_counts_by_city
        WITH (PARTITIONS=64) AS
        SELECT country, city, count(*)
        FROM pageviews_meaningful_with_user_info
        GROUP BY country, city;

KSQL
++++

Since the example messages are small, you can expect KSQL to be CPU-bound. To estimate the throughput from each KSQL server, first estimate the throughput each query would get from a single server if run in isolation. The rule-of-thumb heuristic is that the join will consume about twice the CPU of the project/filter. In this hypothetical environment, you can estimate 25 MB/s for it. Aggregations consume around twice the CPU of joins, so you can estimate 12.5 MB/s for the query populating ``pageview_counts_by_city``.

To estimate the cumulative throughput from this pipeline, you can use the following:

- The KSQL nodes are CPU-bound and for a query to process R bytes/second each byte consumes 1/R CPU-seconds.
- 3 queries with rates R1, R2, and R3 are processing one record for each query, which takes 1/R1 + 1/R2 + 1/R3 CPU-seconds.
- The expected throughput should be 1/(1/R1 + 1/R2 + 1/R3).

Calculating these rates gives an expected throughput of approximately 7.7 MB/s. So you would need about seven 4-core KSQL nodes.

To calculate how much memory is required per server, consider the following:
- You should reserve 8 GB for the Java heap. 
- You must account for up-front state store memory overhead. 

Across the join and aggregate, create 128 state store instances, one for each partition. Each state store allocates a 50 
MB cache, which adds up to 6.25 GB total, and therefore a little under 1 GB per KSQL server.

To make joins as fast as possible, you should make sure that the users table fits in the page cache. To estimate the size
of users, you need to know the number of registered users and the size of each user record and key. Each entry in the user
table contains a registertime (13 bytes), gender(1 byte), city ID (10 bytes), country ID (10 bytes), user ID (32 bytes),
and email (32 bytes). This adds up to a total of 98 bytes. With JSON overhead, you can estimate 123 bytes. The key for
the table is the user ID, which is estimated at 32 bytes. If your site has 100,000,000 registered users, it will require
approximately 14.4 GB to store your whole table, and about 2.1 GB per KSQL server.

To make aggregation as fast as possible, you should ensure that all of the aggregates fit in the page cache. To estimate
the size of the aggregates, you need the number of aggregates and the size of the state required to store each one. Each
aggregate requires storing a country ID (10 bytes), city ID (10 bytes) and count (20 bytes), adding up to 40 bytes. With
overhead, you can estimate 50 bytes. The number of the aggregates is the number of cities with registered users. You can
estimate 50,000 cities. To store all the aggregates will require approximately 2.4 MB of memory, which is negligible.

Each KSQL server should have at least about 12 GB of memory.

Kafka
+++++

KSQL would create 5 new topics (3 output topics, 1 repartition topic, and 1 changelog topic), each with 64 partitions.
You would have to account for 256 additional partitions in the Kafka cluster.

