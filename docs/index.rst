.. _ksql_home:

KSQL
====

.. toctree::
   :titlesonly:
   :maxdepth: 1
   :hidden:

   installation/index
<<<<<<< HEAD
   tutorials/index
   operations
   syntax-reference
=======
   operations
   syntax-reference
   ksql-clickstream-demo
   examples
>>>>>>> eb698ace... WIP
   faq

What Is KSQL?
-------------

KSQL is an open source streaming SQL engine for Apache Kafka®. It provides a simple interactive SQL interface for stream
processing on Kafka, without the need to write code in a programming language such as Java or Python. KSQL is scalable, reliable,
and real-time. It supports a wide range of streaming operations, including aggregations, joins, windowing, and sessionization.

Learn More
    Watch the `screencast of the KSQL demo <https://www.youtube.com/embed/A45uRzJiv7I>`_ on YouTube.

    .. raw:: html

          <div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; height: auto;">
              <iframe src="https://www.youtube.com/embed/A45uRzJiv7I" frameborder="0" allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
          </div>

What Can I Do With KSQL?
------------------------

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

What Are the Components?
------------------------

KSQL Server
    The KSQL server runs the engine that executes KSQL queries. This includes processing, reading, and writing data to and from
    the target Kafka cluster.

    Servers can run in containers, virtual machines, and bare-metal machines. You can add or remove multiple servers in the
    same resource pool to elastically scale query processing in or out. You can use different resource pools to achieve workload
    isolation.

CLI
    You can interactively write KSQL queries by using the KSQL command line interface (CLI). The KSQL CLI acts as a client
    to the KSQL server.

Web Interface
    The KSQL web interface provides a simple visual wrapper for the interactive KSQL CLI. The web interface is intended for local
    development, testing, and demoing. It is not recommended for production environments and will likely change in the future.

    - By default, the KSQL web interface runs on every KSQL Server and is accessible at `http://ksqlserver:8080 <http://ksqlserver:8080>`_.
    - The web interface shares the port with KSQL's REST API, which you can configure via the listeners configuration property.


How Do I Get Started?
---------------------

|cp| Quick Start
    This :ref:`quickstart` will get you up and running with |cp| and its main components, including using |c3-short| to create, add, and modify topic data, and using KSQL to configure stream processing on Kafka.

KSQL Tutorials
    - :ref:`Using KSQL to write streaming queries against data in Kafka <ksql_quickstart>`. This tutorial demonstrates a simple workflow using KSQL to write streaming queries against messages in Kafka.
    - :ref:`Using KSQL to Collect, Analyze, and Report Aggregate Data <ksql_clickstream>`. Learn how to use KSQL, ElasticSearch, and Grafana to analyze data feeds and build a real-time dashboard for reporting and alerting.



