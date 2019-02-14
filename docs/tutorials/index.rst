.. _ksql_tutorials:

KSQL Tutorials and Examples
===========================

.. toctree::
    :hidden:

    basics-docker
    basics-local
    clickstream-docker
    generate-custom-test-data
    examples

KSQL Basics
***********

This tutorial demonstrates a simple workflow using KSQL to write streaming queries against messages in Kafka.

Write Streaming Queries with the KSQL CLI
-----------------------------------------

.. include:: ../includes/ksql-includes.rst
  :start-after: CLI_welcome_start
  :end-before: CLI_welcome_end

Get started with the KSQL CLI:

- :ref:`ksql_quickstart-docker`
- :ref:`ksql_quickstart-local`

Write Streaming Queries with KSQL and |c3|
------------------------------------------

.. image:: ../../../images/ksql-interface-create-stream.png
     :width: 600px

Get started with KSQL and |c3|:

- |c3-short| :ref:`deployed with Docker <ce-docker-ksql-quickstart>` 
- |c3-short| :ref:`deployed locally <ce-ksql-quickstart>`

Stream Processing Cookbook
**************************

The `Stream Processing Cookbook <https://www.confluent.io/product/ksql/stream-processing-cookbook>`__
contains KSQL recipes that provide in-depth tutorials and recommended deployment scenarios.

Clickstream Data Analysis Pipeline
**********************************

Clickstream analysis is the process of collecting, analyzing, and
reporting aggregate data about which pages a website visitor visits and
in what order. The path the visitor takes though a website is called the
clickstream.

This tutorial focuses on building real-time analytics of users to determine:

* General website analytics, such as hit count and visitors
* Bandwidth use
* Mapping user-IP addresses to actual users and their location
* Detection of high-bandwidth user sessions
* Error-code occurrence and enrichment
* Sessionization to track user-sessions and understand behavior (such as per-user-session-bandwidth, per-user-session-hits etc)

The tutorial uses standard streaming functions (i.e., min, max, etc) and enrichment using child tables, stream-table join, and different
types of windowing functionality.

Get started now with these instructions:
    
- :ref:`ksql_clickstream-docker`

If you do not have Docker, you can also run an `automated version <https://github.com/confluentinc/examples/tree/master/clickstream>`_ of the Clickstream tutorial designed for local Confluent Platform installs. Running the Clickstream demo locally without Docker requires that you have Confluent Platform installed locally, along with Elasticsearch and Grafana.

KSQL Examples
*************

:ref:`These examples <ksql_examples>` provide common KSQL usage operations.

.. include:: ../../../includes/connect-streams-pipeline-link.rst
    :start-line: 2
    :end-line: 6

KSQL in a Kafka Streaming ETL
*****************************

To learn how to deploy a Kafka streaming ETL using KSQL for stream processing, you can run the :ref:`Confluent Platform demo<cp-demo>`. All components in the |cp| demo have encryption, authentication, and authorization configured end-to-end.

Level Up Your KSQL Videos
*************************

+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| Video                                                                                        | Description                                                                      |
+==============================================================================================+==================================================================================+
| `KSQL Introduction <https://www.youtube.com/embed/C-rUyWmRJSQ>`_                             | Intro to Kafka stream processing, with a focus on KSQL.                          |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `KSQL Use Cases <https://www.youtube.com/embed/euz0isNG1SQ>`_                                | Describes several KSQL uses cases, like data exploration, arbitrary filtering,   |
|                                                                                              | streaming ETL, anomaly detection, and real-time monitoring.                      |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `KSQL and Core Kafka <https://www.youtube.com/embed/-GpbMAK3Uow>`_                           | Describes KSQL dependency on core Kafka, relating KSQL to clients, and describes |
|                                                                                              | how KSQL uses Kafka topics.                                                      |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Installing and Running KSQL <https://www.youtube.com/embed/icwHpPm-TCA>`_                   | How to get KSQL, configure and start the KSQL server, and syntax basics.         |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `KSQL Streams and Tables <https://www.youtube.com/embed/DPGn-j7yD68>`_                       | Explains the difference between a STREAM and TABLE, shows a detailed example,    |
|                                                                                              | and explains how streaming queries are unbounded.                                |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Reading Kafka Data from KSQL <https://www.youtube.com/embed/EzVZOUt9JsU>`_                  | How to explore Kafka topic data, create a STREAM or TABLE from a Kafka topic,    |
|                                                                                              | identify fields. Also explains metadata like ROWTIME and TIMESTAMP, and covers   |
|                                                                                              | different formats like Avro, JSON, and Delimited.                                |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Streaming and Unbounded Data in KSQL <https://www.youtube.com/embed/4ccg1AFeNB0>`_          | More detail on streaming queries, how to read topics from the beginning, the     |
|                                                                                              | differences between persistent and non-persistent queries, how do streaming      |
|                                                                                              | queries end.                                                                     |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Enriching data with KSQL <https://www.youtube.com/embed/9_Gwe6qJrjI>`_                      | Scalar functions, changing field types, filtering data, merging data with JOIN,  |
|                                                                                              | and rekeying streams.                                                            |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Aggregations in KSQL <https://www.youtube.com/embed/db5SsmNvej4>`_                          | How to aggregate data with KSQL, different types of aggregate functions like     |
|                                                                                              | COUNT, SUM, MAX, MIN, TOPK, etc, and windowing and late-arriving data.           |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Taking KSQL to Production <https://www.youtube.com/embed/f3wV8W_zjwE>`_                     | How to use KSQL in streaming ETL pipelines, scale query processing, isolate      |
|                                                                                              | workloads, and secure your entire deployment.                                    |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
| `Monitoring KSQL in Confluent Control Center <https://www.youtube.com/watch?v=3o7MzCri4e4>`_ | Monitor performance and end-to-end message delivery of your KSQL queries.        |
+----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
