.. _ksql_capacity_planning:

======================
KSQL Capacity Planning
======================

.. contents::

KSQL is a simple and powerful tool for building streaming applications on top of Kafka. This guide will help you get started with planning for provisioning your KSQL deployment by helping you answer questions like:

    - What server specification should I use to run KSQL?
    - Roughly how many KSQL server nodes will I need?
    - Do I need to provision additional Kafka brokers to support KSQL?
    - What kind of deployment mode makes sense for me?
    - How do I know whether my KSQL queries are handling the incoming message rate, and how can I tune KSQL if it’s falling behind?

Background
==========

Before jumping into the following sections, you may find it instructive to look through the `Kafka Streams capacity planning guide <https://docs.confluent.io/current/streams/sizing.html>`_. Under the hood KSQL uses the `Kafka Streams API <https://docs.confluent.io/current/streams/developer-guide/dsl-api.html>`_  to perform the actual query processing, so the details covered there apply to KSQL as well.

Approach To Sizing
==================

This guide will help you get a rough idea of the computing resources required to run your KSQL queries. The emphasis here is that it’s a rough idea. As you’ll see in the following sections there are many factors at play and the best practice is always to stage and test your queries against realistic load first, and adjust your planned provisioning accordingly.

General Guidelines
==================

KSQL
----

**CPU**: KSQL consumes CPU to serialize/deserialize messages into the declared stream/table schemas, and then process each message as required by the query. We would recommend at least 4 cores.

**Disk**: KSQL uses local disk to persist temporary state for aggregations and joins. We recommend using an SSD and starting with at least 100GB.

**Memory**: KSQL memory usage is dominated by on-heap message processing and off-heap state for aggregations (e.g., SUM, COUNT, TOPKDISTINCT) and joins. Usage for message processing scales with message throughput, while aggregation/join state is a function of topic partition count, key space size, and windowing. A good starting point here is 32GB.

**Network**: KSQL relies heavily on Kafka, so fast and reliable network is important for optimal throughput. A 1Gib NIC is a good starting point.

To recap, general guidelines for a basic KSQL server:
    - 4 cores
    - 32GB RAM
    - 100GB SSD
    - 1Gbit network

Kafka
-----

KSQL consumes resources on your Kafka cluster.

Internal Topics
+++++++++++++++

KSQL creates the following types of topics on your KSQL cluster:

**Output Topics**

Every query started by a ``CREATE STREAM AS SELECT`` or ``CREATE TABLE AS SELECT`` statement writes its results to an output topic. The created topic is configured with the following properties:

    - Name: By default KSQL creates the output topic with the same name as the Stream or Table created by the statement. Users can specify a custom name in the ``KAFKA_TOPIC`` property of the statement's ``WITH`` clause.
    - Partitions: By default KSQL creates an output topic with 4 partitions. Users can specify a custom partition count in the ``PARTITIONS`` property of the statement's ``WITH`` clause.
    - Replication Factor: By default KSQL creates the output topic with a replication factor of 1. Users can specify a custom replication factor in the ``REPLICAS`` property of the statement's ``WITH`` clause.

**Internal Topics for Repartitioning**

Some queries require that the input Stream be repartitioned so that all messages being aggregated or joined together reside in the same partition. Repartitioning means that an intermediate topic is created and every record is produced and consumed to/from that topic with a key that ensures the locality requirements of the query are satisfied. The intermediate topic is named with the suffix *repartition*. The repartition topic has the same number of partitions and replicas as the input Stream for aggregations, or the input Table for joins.

Tip
    To determine if your query needs a repartition, you can use the ``DESCRIBE EXTENDED`` and ``EXPLAIN`` statements.
    For example, consider the following table created from the quickstart:

    .. code:: bash

        ksql> CREATE TABLE pageviews_by_page AS SELECT pageid, COUNT(*) FROM pageviews_original GROUP BY pageid;
        ksql> DESCRIBE EXTENDED pageviews_by_page;
        ...
        Queries that write into this TABLE
        -----------------------------------
        id:CTAS_PAGEVIEWS_BY_PAGE - CREATE TABLE pageviews_by_page AS SELECT pageid, COUNT(*) FROM pageviews_original GROUP BY pageid;

        For query topology and execution plan please run: EXPLAIN <QueryId>

    The DESCRIBE EXTENDED output includes the Query ID for the query populating the table. You can run EXPLAIN against the query to print the underlying streams topology:

    .. code:: bash

        ksql> EXPLAIN CTAS_PAGEVIEWS_BY_PAGE;

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

When run in interactive configuration, a KSQL cluster creates an internal topic (whose name is derived from the ksql.service.id setting) to persist the log of queries to run across all the servers in this KSQL cluster. These special-purpose topics for KSQL are called command topics.  Command topics have a single partition and default to a replication factor of 1.

Consumption/Production
++++++++++++++++++++++

You may need to provision additional Kafka brokers to accommodate KSQL production/consumption to/from your Kafka cluster.

At minimum, each query consumes each record from an input Kafka topic and produces records to an output Kafka topic.

Some queries require that the input Stream be repartitioned so that all messages being aggregated or joined together reside in the same partition. Each repartition produces and consumes every record.

Finally, stateful queries such as aggregations and joins produce records to a changelog topic for their respective state stores. 

Important Sizing Factors
========================

In this section we’ll go over some of the important factors to consider when scoping out your KSQL deployment.

**Throughput**: In general, higher throughput requires more resources.

**Query Types**: Your realized throughput will largely be a function of the type of queries you run. You can think of KSQL queries as falling into three categories:

    1. Project/Filter, e.g. ``SELECT <columns> FROM <table/stream> WHERE <condition>``
    2. Joins
    3. Aggregations, e.g. ``SUM, COUNT, TOPK, TOPKDISTINCT``

A project/filter query reads records from an input stream or table, may filter the records according to some predicate, and performs stateless transformations on the columns before writing out records to a sink stream or table. Project/filter queries require the fewest resources. For a single project/filter query running on an instance provisioned as recommended above you can expect to realize from ~40 MB/second up to the rate supported by your network. The throughput largely depends on the average message size and complexity. Processing small messages with many columns is CPU intensive and will saturate your CPU. Processing large messages with fewer columns requires less CPU and KSQL will start saturating the network for such workloads.

Stream-table joins read and write to Kafka Streams state stores and require around twice the CPU of project/filter. Though Kafka Streams state stores are stored on disk, we recommend that you provision sufficient memory to keep the working set memory-resident to avoid expensive disk i/os. So expect around half the throughput and expect to provision higher-memory instances.

Aggregations read from and may write to a state store for every record. They consume around twice the CPU of joins. The CPU required increases if the aggregation uses a window as the state store must be updated for every window.

**Number of Queries**: The available resources on a server are shared across all queries. So expect that the processing throughput per server will decrease proportionally with the number of queries it is executing (see the notes on vertically/horizontally scaling a KSQL cluster in this document to add more processing capacity in such situations) . Furthermore, KSQL queries run as Kafka Streams applications. Each query starts its own Kafka Streams worker threads, and uses its own consumers and producers. This adds a little bit of CPU overhead per query. Currently we recommend avoiding running a large number of queries on one ksql cluster. Instead, use interactive mode to play with your data and develop sets of queries that function together. Then, run these in their own headless cluster. Check out the Recommendations and Best practices section for more details.

**Data Schema**: KSQL handles mapping serialized Kafka records to columns in a stream or table’s schema. In general, more complex schemas with a higher ratio of columns to bytes of data require more CPU to process.

**Number of Partitions**: Kafka Streams creates one RocksDB state store instance for aggregations and joins for every topic partition processed by a given KSQL server. Each RocksDB state store instance has a memory overhead of 50MB for its cache plus the data actually stored.

**Key Space**: For aggregations and joins, Kafka Streams/RocksDB will try and keep the working set of a state store in memory to avoid I/O operations. If there are many keys then this will require more memory. It also makes reads and writes to the state store more expensive. Note that the size of the data in a state store is not limited by memory (RAM) but only by available disk space on a KSQL server.

Recommendations and Best Practices
==================================

Interactive KSQL Servers vs Non-Interactive (“Headless”) Servers 
----------------------------------------------------------------

By default, KSQL servers are configured for interactive use, which means you can use the KSQL CLI to interact with a KSQL cluster in order to, for example, execute new queries. Interactive KSQL usage allows for easy and quick iterative development and testing of your KSQL queries via the KSQL CLI.

You can also configure the servers for headless, non-interactive operation, where servers collaboratively run only a predefined list of queries [link to KSQL server configuration section]. The result is essentially an elastic, fault-tolerant, and distributed stream processing application that communicates to the outside world by reading from and writing to Kafka topics.  Sizing, deploying, and managing in this scenario is similar to a Kafka Streams application [link to KStreams docs] . We also recommend to integrate such KSQL deployments with your company's CI/CD pipeline to, for example, version-control the .sql file.

How to choose between these two configuration types?

    - For production deployments we recommend headless, non-interactive KSQL clusters. This configuration provides the best isolation and, unlike interactive KSQL clusters, minimizes the likelihood of operator error and human mistakes.
    - For exploring and experimenting with your data we recommend interactive KSQL clusters. It allows to quickly come up with sets of desired queries for your use case that together function as a streaming “application” to produce meaningful results. Then, run this “application” with headless, non-interactive KSQL clusters in production.
    - Furthermore, for interactive KSQL usage we recommend that you deploy an interactive KSQL cluster per project or per team instead of a single, large KSQL cluster for your organization.

Scaling KSQL
------------

You can scale KSQL vertically (more capacity per server) or horizontally (more servers). KSQL clusters can be elastically scaled during live operations without loss of data, i.e. you can add and remove KSQL servers in order to increase or decrease processing capacity. When scaling vertically, configure servers with a larger number of stream threads (see ``ksql.streams.num.stream.threads`` in the KSQL configuration documentation). Beyond approximately 8 cores it’s generally recommended to scale horizontally by adding servers.

Like Kafka Streams, KSQL throughput scales well as resources are added, so long as your Kafka topics have enough partitions to increase parallelism. For example, if your input topic has 5 partitions, the maximum parallelism is also 5. This means a maximum of 5 cores/threads would execute a query on this topic in parallel To increase the maximum level of parallelism, you must increase the number of partitions that are being processed, for which you have two options: (1) you can re-partition your input data into a new stream with the ``CREATE STREAM AS SELECT`` statement and then write subsequent queries against the repartitioned stream (in which case you may consider lowering the data retention configuration for that stream’s underlying topic if you want to save storage space in your Kafka cluster), or (2) you can directly increase the number of partitions in the input topic.

How to Know When to Scale
+++++++++++++++++++++++++

If KSQL is not able to keep up with the production rate of your Kafka topics, it will start to fall behind in processing the incoming data. Consumer lag is the Kafka terminology for describing how much a Kafka consumer including KSQL has fallen behind. It’s important to monitor consumer lag on your topics and add resources if you observe that the lag is growing. We recommend `Confluent Control Center <https://docs.confluent.io/current/control-center/docs/index.html>`_ for such montoring. You can also check out our `Kafka documentation <https://docs.confluent.io/current/kafka/monitoring.html>`_ for details on metrics exposed by Kafka that can be used to monitor lag.

Mixed Workloads
+++++++++++++++

Your workload may involve multiple queries, perhaps with some feeding data into others in a streaming pipeline. For example, a project/filter to transform some data that is then aggregated. Monitoring consumer lag of each query’s input topic is especially important for such workloads. KSQL currently does not have a mechanism to guarantee resource utilization fairness between queries. So a faster query like a project/filter may “starve” a more expensive query like a windowed aggregate if the production rate into the source topics is high. If this happens you will observe growing lag on the source topic for the more expensive query(ies) and very low throughput to their sink topics.

There are 2 ways to remedy this situation. One option is to tune the cheaper query(ies) to consume less CPU by decreasing ``kafka.streams.num.threads`` for that query. The other alternative is to add resources to reduce the per-CPU usage of the cheaper query(ies), which in turn will increase the throughput for the more expensive queries.

Examples
========

This section goes over some sizing scenarios to give you more concrete examples of how to think about sizing. We assume a use case where we are interested in analyzing a stream of pageview events.

    Note: The environment and numbers in this section are hypothetical and only meant for illustration purposes. We recommend to perform your own benchmarking and testing to match your use cases and environments.

The examples assume the following DDL statements to declare the schema for the input data:

    .. code:: sql

        CREATE STREAM pageviews_original
            (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, client_ip INT, url VARCHAR, duration BIGINT, from_url VARCHAR, analytics VARCHAR)
            WITH (kafka_topic='pageviews', value_format=’JSON’, KEY=’userid’);

        CREATE TABLE users (registertime BIGINT, gender VARCHAR, city INT, country INT, userid VARCHAR, email VARCHAR)
            WITH (kafka_topic='users', value_format='JSON', key = 'userid');

Let’s assume the following:

    - The production rate into the ``pageviews`` topic is 50 MBps.
    - The messages in ``pageviews`` average 256 bytes.
    - The ``pageviews`` topic has 64 partitions.
    - The messages are in JSON format. Serialization to JSON adds some space overhead. Let’s assume an extra 25% to account for this.

Scenario 1: Project/Filter Only (Stateless Queries)
---------------------------------------------------

In this scenario my application is a single project/filter query that tries to capture meaningful pageviews by filtering out all the views that lasted less than 10 seconds:

    .. code:: sql

        CREATE STREAM pageviews_meaningful
            WITH (PARTITIONS=64) AS
            SELECT *
            FROM pageviews_original
            WHERE duration > 10;

KSQL
++++

Our example pageviews messages are small, under 256 bytes. For smaller messages let’s estimate that, in our hypothetical environment, each 4-core KSQL server is CPU bound at around 50MBps. So, we should be able to get by with a single KSQL server to handle this throughput. For better fault-tolerance it may be a good idea to run a second server to quickly pick up load in case one server fails.

How much memory is required per server? Project/Filter is stateless, and therefore we do not need to account for state store memory. We recommend about 8GB for the Java heap space for record processing.

Finally, KSQL uses the network to consume records from the Kafka input topic and produce records to the output topic. So for this example query we said we receive 50MBps. If we assume that 90% of page views are meaningful then we would produce 45MBps as output.

Kafka
+++++

On the Kafka side we would need to provision for the additional production/consumption bandwidth as calculated above. Additionally, we would need to account for the output topic itself, which would add 64 partitions to the Kafka cluster.

Scenario 2: Large Messages
--------------------------

Suppose we want to perform the same query, except this time each message is 8KB. For larger messages, each KSQL node will likely be network bound instead of CPU bound. One node with a 1Gbps should be able to handle the original 50MBps (400Mbps) of throughput coming into the pageviews_original topic. Let’s suppose the production throughput is larger, 256MBps. A 1Gbps full-duplex NIC can handle 1Gbps, or 128MBps in each direction. So we would estimate 2-3 KSQL nodes to handle this load.

Scenario 3: More Advanced Usage
-------------------------------

Let’s analyze a more interesting set of queries. Let’s assume small messages again (256 bytes). Let’s say we want to enrich ``pageviews_meaningful`` with information about the user, and then count up views by city:

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

Since our example messages are small, we expect KSQL to be CPU-bound. To estimate the throughput from each KSQL server, we first estimate the throughput each query would get from a single server if run in isolation. Remember our rule-of-thumb heuristic that the join will consume about twice the CPU of the project/filter. So in our hypothetical environment we estimate 25MBps for it. Aggregations consume around twice the CPU of joins, so lets estimate 12.5MBps for the query populating ``pageview_counts_by_city``. To estimate the cumulative throughput from this pipeline, we can use the following reasoning. Since we’re CPU-bound, for a query to process R bytes/second each byte consumes 1/R CPU-seconds. If we have 3 queries with rates R1, R2, and R3 then processing one record for each query takes 1/R1 + 1/R2 + 1/R3 CPU-seconds. So the expected throughput should be 1/(1/R1 + 1/R2 + 1/R3). Plugging in our rates gives us an expected throughput of ~7.7MBps. So we would need around 7 4-core KSQL nodes.

Now let’s see how much memory we require per server. We recommend that you reserve 8GB for the java heap. We also need to account for up front state store memory overhead. Across the join and aggregate we create 128 state store instances, one for each partition. Each state store allocates a 50MB cache, which comes to 6.25GB total and therefore a little under 1GB per KSQL server.

To make joins as fast as possible, we should try and ensure that the users table fits in the page cache. To estimate the size of users, we need to know the number of registered users and the size of each user record and key. Each entry in the user table contains a registertime (13 bytes), gender(1 byte), city id (10 bytes), country id (10 bytes), userid (32 bytes), and email (32 bytes), coming to a total of 98 bytes. With JSON overhead we estimate 123 bytes. The key for the table is the userid, for which we estimate 32 bytes. Suppose our site has 100,000,000 registered users. Then, to store our whole table will require around 14.4GB, and therefore about 2.1GB per KSQL server.

To make aggregation as fast as possible we should try and ensure all the aggregates fit in the page cache. To estimate the size of the aggregates, we need to know the number of aggregates and the size of the state required to store each one. Each aggregate requires storing a country ID (10 bytes), city ID (10 bytes) and count (20 bytes), coming to 40 bytes. With overhead we estimate 50 bytes. The number of the aggregates is the number of cities with registered users. Lets estimate 50,000 cities. Then, to store all the aggregates will require around 2.4MB of memory, which is negligible.

So, each KSQL server should therefore have at least about 12 GB of memory.

Kafka
+++++

KSQL would create 5 new topics (3 output topics, 1 repartition topic, and 1 changelog topic), each with 64 partitions. So we’d need to account for 256 additional partitions in the Kafka cluster.

