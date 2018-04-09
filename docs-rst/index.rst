.. _ksql_home:

KSQL
====

.. toctree::
   :titlesonly:
   :maxdepth: 1
   :hidden:

   quickstart
   installation/index
   operations
   tutorials/index
   syntax-reference
   faq

.. raw:: html

   <h2>What Is KSQL?</h2>

---------------------

KSQL is the open source streaming SQL engine for Apache Kafka®. It provides an easy-to-use yet powerful interactive SQL
interface for stream processing on Kafka, without the need to write code in a programming language such as Java or
Python. KSQL is scalable, elastic, fault-tolerant, and real-time. It supports a wide range of streaming operations,
including data filtering, transformations, aggregations, joins, windowing, and sessionization.

.. include:: includes/ksql-includes.rst
    :start-line: 8
    :end-line: 17

.. raw:: html

   <h2>What Can I Do With KSQL?</h2>

---------------------

Streaming ETL
    Apache Kafka is a popular choice for powering data pipelines. KSQL makes it simple to transform data within the pipeline, readying messages to cleanly land in another system.

Real-time Monitoring and Analytics
    Track, understand, and manage infrastructure, applications, and data feeds by quickly building real-time dashboards, generating metrics, and creating custom alerts and messages.

Data exploration and discovery
    Navigate and browse through your data in Kafka.

Anomaly detection
    Identify patterns and spot anomalies in real-time data with millisecond latency, allowing you to properly surface out of the ordinary events and to handle fraudulent activities separately.

Personalization
    Create data driven real-time experiences and insight for users.

Sensor data and IoT
    Understand and deliver sensor data how and where it needs to be.

Customer 360-view
    Achieve a comprehensive understanding of your customers across every interaction through a variety of channels, where new information is continuously incorporated in real-time.

.. raw:: html

   <h2>What Are the Components?</h2>

---------------------

KSQL Server
    The KSQL server runs the engine that executes KSQL queries. This includes processing, reading, and writing data to
    and from the target Kafka cluster.

    KSQL servers form KSQL clusters and can run in containers, virtual machines, and bare-metal machines. You can add
    and remove servers to/from the same KSQL cluster during live operations to elastically scale KSQL's processing
    capacity as desired. You can deploy different KSQL clusters to achieve workload isolation.

KSQL CLI
    You can interactively write KSQL queries by using the KSQL command line interface (CLI). The KSQL CLI acts as a
    client to the KSQL server. For production scenarios you may also configure KSQL servers to run in non-interactive
    "headless" configuration, thereby preventing KSQL CLI access.
