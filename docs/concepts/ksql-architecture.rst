.. _ksql-architecture:

KSQL Architecture
#################

The architecture of KSQL enables you to build real-time streaming applications
from Kafka topics by using only SQL statements and queries. KSQL is built on
Kafka Streams, so a KSQL application runs on a  Kafka cluster like any other
Kafka Streams application.

KSQL Components
***************

KSQL has three main components: the engine, the REST API, and the command-line
interface (CLI).

KSQL Server comprises the KSQL engine and the REST API.

Use the KSQL CLI to interact with KSQL Server instances and develop your
streaming applications.

.. image:: ../img/ksql-architecture-and-components.png
   :alt: Diagram showing architecture of KSQL
   :align: center

KSQL Engine
    The KSQL engine executes KSQL statements and queries. Each KSQL server
    instance runs a KSQL engine. 
    which means that it runs Kafka Streams topologies.
    which processes KSQL queries,
    Engine implementation: `KsqlEngine.java <https://github.com/confluentinc/ksql/blob/master/ksql-engine/src/main/java/io/confluent/ksql/KsqlEngine.java>`__

KSQL CLI
    The KSQL CLI provides a console with a command-line interface for the KSQL engine. that's designed to be
    familiar to users of MySQL, Postgres etc.
    CLI implementation: `io.confluent.ksql.cli <https://github.com/confluentinc/ksql/tree/master/ksql-cli/src/main/java/io/confluent/ksql/cli>`__

REST Interface
    The REST server interface enables communicating with a KSQL engine from
    the CLI or any other REST client.
    which enables access to the engine.
    REST server implementation: `KsqlRestApplication.java <https://github.com/confluentinc/ksql/blob/master/ksql-rest-app/src/main/java/io/confluent/ksql/rest/server/KsqlRestApplication.java>`__

When you deploy your KSQL application, it runs on KSQL Server instances that
are independent of one another, are fault-tolerant, and can scale elastically
with load.

.. image:: ../img/ksql-server-scale-out.gif
   :alt: Diagram showing architecture of KSQL
   :align: center


KSQL and Kafka Streams
**********************

KSQL is built on Kafka Streams and occupies the top of the stack in |cp|.
For more information on their relationship, see :ref:`ksql-and-kafka-streams`.
For more information on Kafka Streams, see :ref:`streams_architecture`.

KSQL Language Elements
**********************

Like traditional relational databases, KSQL supports two categories of
statements: Data Definition Language (DDL) and Data Manipulation Language (DML).

These two categories are similar in syntax, data types, and expressions, but
they have different functions on a KSQL server.

Data Definition Language (DDL) Statements
    Imperative verbs that define metadata on the KSQL server by adding,
    changing, or deleting streams and tables. Data Definition Language
    statements modify metadata only and don't operate on data. You can use
    these statements with declarative DML statements.

    The DDL statements include:

    * CREATE TABLE
    * CREATE STREAM
    * DROP STREAM
    * DROP TABLE

Data Manipulation Language (DML) Statements
    Declarative verbs that read and modify data in KSQL streams and tables.
    Data Manipulation Language statements modify data only and don't change
    metadata. The KSQL engine compiles DML statements into Kafka Streams
    applications, which run on a Kafka cluster like any other Streams application.

    The DML statements include:

    * CREATE TABLE AS SELECT (CTAS)
    * CREATE STREAM AS SELECT (CSAS)
    * SELECT
    * INSERT INTO


KSQL Deployment Modes
*********************

https://docs.google.com/presentation/d/1CU2-r2ZiSG_cTa1UqFq4ZwJnq7imr89pXkJVYAlecp4/edit#slide=id.g49c8886607_0_80

Interactive
=========== 

.. image:: ../img/ksql-client-server-interactive-mode.png
   :alt: Diagram showing interactive KSQL deployment
   :align: center

Start any number of server nodes
bin/ksql-server-start
Start one or more CLIs or REST Clients and point them to a server
bin/ksql https://myksqlserver:8090
All servers share the processing load
Technically, instances of the same Kafka Streams Applications
 scale up / down without restart

Headless
========

.. image:: ../img/ksql-standalone-headless.png
   :alt: Diagram showing headless KSQL deployment
   :align: center


Start any number of server nodes
Pass a file of KSQL statement to execute
bin/ksql-node query-file=foo/bar.sql
Ideal for streaming ETL application deployment
Version-control your queries and transformations as code
All running engines share the processing load
Technically, instances of the same Kafka Streams Applications
 scale up / down without restart

Leave resource mgmt. to dedicated systems such as k8s
All running Engines share the processing load
Technically, instances of the same Kafka Streams Applications
Scale up/down without restart



Embedded
========


.. image:: ../img/ksql-embedded-in-application.png
   :alt: Diagram showing KSQL embedded in an application
   :align: center


Embed directly in your Java application
Generate and execute KSQL queries through the Java API
Version-control your queries and transformations as code
All running application instances share the processing load
Technically, instances of the same Kafka Streams Applications
 scale up / down without restart

Here, you are just deploying a JVM-based application using the application
framework of your choosing: Spring, Grails, Jersey, VertX, Ratpack, or whatever.
You want that application to be able to execute KSQL queries without spinning up
a separate KSQL cluster. You can embed the engine itself into the app, then scale
the app (and its stream processing) the way you would normally scale a Streams app
or a KSQL cluster. It’s a consumer group, and gets all that magic for free.


Dedicating Resources
====================

.. image:: ../img/ksql-dedicating-resources.png
   :alt: Diagram showing how to join KSQL engines to the same service pool
   :align: center

Join Engines to the same ‘service pool’ by means of the ksql.service.id property

Deployment
Two modes: non-interactive, aka "headless", 
Long-running production deployments
Resource isolation
No REST API
KSQL servers with a SQL file


Interactive service, which enables data exploration and pipeline design.
REST API
Command topic
(animated gif)


To scale out, just add more KSQL server instances. There's no master node or 
coordination among them required.

For more information, see :ref:`ksql_capacity_planning`.

KSQL Query Lifecycle
********************

#. You register the stream, e.g., CREATE STREAM <my-stream> WITH <topic-name>
#. You express your app by using a KSQL statement, e.g., CREATE TABLE AS SELECT
   FROM <my-stream>
#. KSQL parses your statement into an abstract syntax tree (AST)
#. KSQL uses the AST to create the logical plan for your statement
#. KSQL the logical plan to create the physical plan for your statement
#. KSQL generates and runs the Kafka Streams application.
#. You manage the application as a STREAM or TABLE and a corresponding
   persistent query.

.. image:: ../img/ksql-query-lifecycle.gif
   :alt: Diagram showing how the KSQL query lifecycle for a KSQL statement
   :align: center

Register the Stream
===================

.. code:: sql

    CREATE STREAM authorization_attempts 
      (card_number VARCHAR, attemptTime BIGINT, ...)
      WITH (kafka_topic='authorizations', value_format=‘JSON’);


DDL statement is written to the command topic
Each server reads the DDL statement
Parse/analyze the statement -- action is to update the KSQL metastore
each server has an internal in-memory metastore that they build when they receive DDL statements
Add an entry to the metastore 
metastore is an in-memory map

Metastore implementation: `io.confluent.ksql.metastore <https://github.com/confluentinc/ksql/tree/master/ksql-metastore/src/main/java/io/confluent/ksql/metastore>`__


+-------------------------+----------------------------------------------------------------------------------+
| Source Name             | Structured Data Source                                                           |
+=========================+==================================================================================+
| AUTHORIZATION_ATTEMPTS  | [DataSourceType: STREAM],                                                        |
|                         | [Schema:(card_number VARCHAR, attemptTime BIGINT, attemptRegion VARCHAR, ...)],  |
|                         | [Key: null],                                                                     |
|                         | [KsqlTopic: AUTHORIZATIONS],                                                     |
|                         | ...                                                                              |
+-------------------------+----------------------------------------------------------------------------------+

Express Your Application as a KSQL Statement
============================================

Now that we have a stream, we want to express our application by using a KSQL
statement. The following DML statement creates a table from the
``authorization_attempts`` stream:

.. code:: sql

    CREATE TABLE possible_fraud AS
      SELECT card_number, count(*)
      FROM authorization_attempts 
      WINDOW TUMBLING (SIZE 5 SECONDS)
      WHERE region = ‘west’
      GROUP BY card_number
      HAVING count(*) > 3; 

The KSQL engine translates the DML statement into a Kafka Streams application,
which reads the source topic continuously, processes records, and when the
condition is met, writes records to the output topic.

KSQL Parses Your Statement
==========================

parser creates an Abstract Syntax Tree

KSQL statement parser is based on `ANTLR <https://www.antlr.org/>`__

Code for the KSQL statement parser: `io.confluent.ksql.parser <https://github.com/confluentinc/ksql/tree/master/ksql-parser/src/main>`__

KSQL Creates the Logical Plan
=============================

The KSQL engine creates the logical plan by using the AST:

#. First step is the source (FROM node in AST)
#. Filter (WHERE clause)
#. Aggregation (GROUP BY), projection (WINDOW)
#. Post-aggregation filter (HAVING applies to result of aggregation)
#. Projection for result

.. image:: ../img/ksql-statement-logical-plan.gif
   :alt: Diagram showing how the KSQL engine creates a logical plan for a KSQL statement
   :align: center

KSQL Creates the Physical Plan
==============================

From the logical plan, KSQL engine creates the physical plan, which is a specific kind of Kafka Streams
application with a schema.

Kafka Streams DSL with schema. 
KStream  → SchemaKStream (KStream + Schema)
KTable     → SchemaKTable (KTable + Schema) 

Traverse the logical plan and create a Kafka Streams application

#. First step is the source (SchemaKStream with info from metastore)
#. Filter, which produces another SchemaKStream
#. Projection, which is the SELECT function
#. Apply aggregation (multiple steps, may need to re-partition data if it's not keyed by GROUP BY phrase) rekey, groupby, aggregate
#. Filter (HAVING)
#. Projection for result (select())

.. image:: ../img/ksql-statement-physical-plan.gif
   :alt: Diagram showing how the KSQL engine creates a physical plan for a KSQL statement
   :align: center



.. graphics-file: https://docs.google.com/presentation/d/1CU2-r2ZiSG_cTa1UqFq4ZwJnq7imr89pXkJVYAlecp4/edit#slide=id.p64
.. graphics-file: https://docs.google.com/presentation/d/1IMBU414rxEt4HrvqvEjjRiyCxMJzcQytC8ypD0dsvTg/edit#slide=id.g4a42e8b1c4_0_19