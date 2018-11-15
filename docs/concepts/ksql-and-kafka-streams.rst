.. _ksql-and-kafka-streams:

KSQL and Kafka Streams
######################

KSQL is the streaming SQL engine for Apache Kafka®. With KSQL, you can write
real-time streaming applications by using a SQL-like query language.

Kafka Streams is the Apache Kafka® library for writing streaming applications
and microservices in Java and Scala.

KSQL is built on Kafka Streams and occupies the top of the stack in |cp|.

.. image:: ../img/ksql-kafka-streams-core-kafka-stack.png

KSQL gives you the highest level of abstraction for implementing real-time
streaming business logic on Kafka topics. KSQL automates much of the complex
programming that's required for real-time operations on streams of data, so
that one line of KSQL can do the work of a dozen lines of Java and Kafka
Streams.

For example, to implement simple fraud-detection logic on a Kafka topic named
``payments``, you could write one line of KSQL:

.. code:: sql

    CREATE STREAM fraudulent_payments AS
     SELECT fraudProbability(data) FROM payments
     WHERE fraudProbability(data) > 0.8;

The equivalent Java code on Kafka Streams might resemble: 

.. code:: java

    // Example fraud-detection logic using the Kafka Streams API.
    object FraudFilteringApplication extends App {

      val builder: StreamsBuilder = new StreamsBuilder()
      val fraudulentPayments: KStream[String, Payment] = builder
        .stream[String, Payment]("payments-kafka-topic")
        .filter((_ ,payment) => payment.fraudProbability > 0.8)
      fraudulentPayments.to("fraudulent-payments-topic")

      val config = new java.util.Properties 
      config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-filtering-app")
      config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092")

      val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
      streams.start()
    }

KSQL is easier to use, and Kafka Streams is more flexible. Which technology
you choose for your real-time streaming applications depends on a number of
considerations. Keep in mind that you can use both KSQL and Kafka Streams
together in your implementations.

The following table summarizes some of the differences between KSQL and Kafka
Streams. 

+-------------------+----------------------+--------------------------------------------+
| Differences       | KSQL                 | Kafka Streams                              |
+===================+======================+============================================+
| You write:        | KSQL statements      | JVM applications                           |
+-------------------+----------------------+--------------------------------------------+
| Graphical UI      | Yes, in |c3|         | No                                         |
+-------------------+----------------------+--------------------------------------------+
| Console           | Yes                  | No                                         |
+-------------------+----------------------+--------------------------------------------+
| Data formats      | Avro, JSON, CSV      | Any data format, including Avro, JSON,     |
|                   |                      | CSV, Protobuf, XML                         |
+-------------------+----------------------+--------------------------------------------+
| REST API included | Yes                  | No, but you can implement your own         |
+-------------------+----------------------+--------------------------------------------+
| Runtime included  | Yes, the KSQL server | Applications run as standard JVM processes |
+-------------------+----------------------+--------------------------------------------+
| Queryable state   | No                   | Yes                                        |
+-------------------+----------------------+--------------------------------------------+

Use the following table to help you decide between KSQL and Kafka Streams as a
starting point for your real-time application development. 

+----------------------------------------------------+------------------------------------------------------+
| Start with KSQL when…                              | Start with Kafka Streams when…                       |
+====================================================+======================================================+
| * New to streaming and Kafka                       | * Prefer writing and deploying JVM applications      |
|                                                    |   like Java and Scala; for example, due to           |
|                                                    |   people skills, tech environment                    |
| * To quicken and broaden the adoption              | * Use case is not naturally expressible through SQL, |
|                                                    |   for example, finite state machines                 |
|   and value of Kafka in your organization          | * Building microservices                             |
| * Prefer an interactive experience with UI and CLI | * Must integrate with external services, or          |
|                                                    |   use 3rd-party libraries (but KSQL UDFs may help)   |
| * Prefer SQL to writing code in Java or Scala      | * To customize or fine-tune a use case, for example, |
| * Use cases include enriching data; joining        |   with the Kafka Streams Processor API:              |
|   data sources; filtering, transforming,           |   custom join variants, or probabilistic counting at |
|   and masking data; identifying anomalous events   |   very large scale with Count-Min Sketch             |
| * Use case is naturally expressible by using SQL,  | * Need queryable state, which KSQL doesn't support   |
|   with optional help from User Defined Functions   |                                                      |
| * Want the power of Kafka Streams but you          |                                                      |
|   aren't on the JVM: use the KSQL REST API         |                                                      |
|   from Python, Go, C#, JavaScript, shell           |                                                      |
+----------------------------------------------------+------------------------------------------------------+

KSQL is usually not a good fit for BI reports and ad-hoc querying, or queries with random access patterns,
because it's a continuous query system on data streams.

Next Steps
**********

* :ref:`ksql_quickstart-docker`
* :ref:`ksql-dev-guide`
* :ref:`streams_developer-guide`
