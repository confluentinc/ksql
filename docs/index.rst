.. _ksql_home:

KSQL
====

.. toctree::
   :maxdepth: 1
   :hidden:

   quickstart/index
   installation/index
   concepts
   syntax-reference
   ksql-clickstream-demo/index
   examples
   faq

.. important::

    This release is a developer preview. It is strongly recommended that you test before running KSQL against a production Kafka cluster.

KSQL is an open source streaming SQL engine for Apache Kafka™. It provides a simple interactive SQL interface for stream
processing on Kafka, without the need to write code in a programming language such as Java or Python. KSQL is scalable, reliable,
and real-time. It supports a wide range of streaming operations, including aggregations, joins, windowing, and sessionization.

Here are some useful resources to get started.

Quick Start
   Create a simple workflow using KSQL and write streaming queries against data in Kafka with the :ref:`ksql_quickstart`.


Clickstream Analysis Demo
   Learn how to analyze data feeds and build a real-time dashboard for reporting and alerting with the :ref:`ksql_clickstream`.

KSQL Screencast
   Watch `a screencast of the KSQL demo <https://www.youtube.com/embed/A45uRzJiv7I>`_ on YouTube.

   .. raw:: html

       <div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; height: auto;">
           <iframe src="https://www.youtube.com/embed/A45uRzJiv7I" frameborder="0" allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
       </div>

Use cases
   Common KSQL use cases are:

   -  Streaming ETL:  Apache Kafka is a popular choice for powering data pipelines. KSQL makes it simple to transform data within the pipeline, readying messages to cleanly land in another system.
   - Real-time Monitoring and Analytics: track, understand, and manage infrastructure, applications, and data feeds by quickly building real-time dashboards, generating metrics, and creating custom alerts and messages.
   - Data exploration and discovery: navigate and browse through your data in Kafka.
   - Anomaly detection: identify patterns and spot anomalies in real-time data with millisecond latency, allowing you to properly surface out of the ordinary events and to handle fraudulent activities separately.
   - Personalization: create data driven real-time experiences and insight for users.
   - Sensor data and IoT: understand and deliver sensor data how and where it needs to be.
   - Customer 360-view: achieve a comprehensive understanding of your customers across every interaction through a variety of channels, where new information is continuously incorporated in real-time.
