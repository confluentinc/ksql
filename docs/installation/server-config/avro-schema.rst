.. _install_ksql-avro-schema:

Configuring Avro and |sr| for KSQL
##################################

KSQL can read and write messages in Avro format by integrating with :ref:`Confluent Schema Registry <schemaregistry_intro>`.
KSQL automatically retrieves (read) and registers (write) Avro schemas as needed and thus saves you from both having to
manually define columns and data types in KSQL and from manual interaction with |sr|.

Supported functionality
***********************

KSQL currently supports Avro data in the Kafka message values. 

Avro schemas with nested fields are supported. In KSQL 5.0 and higher, you can read nested data, in Avro and JSON
formats, by using the STRUCT type. You can’t create new nested STRUCT data as the result of a query, but you can copy existing
STRUCT fields as-is. For more info, see the :ref:`KSQL syntax reference<struct_overview>`.

The following functionality is not supported:

-  Message keys in Avro format are not supported. Message keys in KSQL are always interpreted as STRING format, which means
   KSQL will ignore Avro schemas that have been registered for message keys, and the key will be read using ``StringDeserializer``.

Configuring KSQL for Avro
*************************

You must configure the REST endpoint of |sr| by setting ``ksql.schema.registry.url`` (default: ``http://localhost:8081``)
in the KSQL server configuration file (``<path-to-confluent>/etc/ksql/ksql-server.properties``). For more information,
see :ref:`install_ksql-server`.

.. important:: Do not use the SET statement in the KSQL CLI to configure the registry endpoint.

Using Avro in KSQL
******************

Before using Avro in KSQL, make sure that |sr| is up and running and that ``ksql.schema.registry.url`` is set correctly
in the KSQL properties file (defaults to ``http://localhost:8081``). |sr| is :ref:`included by default <quickstart>` with
|cp|.

Here's what you can do with Avro in KSQL:

- Declare streams and tables on Kafka topics with Avro-formatted data by using ``CREATE STREAM`` and ``CREATE TABLE`` statements.
- Read from and write into Avro-formatted data by using ``CREATE STREAM AS SELECT`` and ``CREATE TABLE AS SELECT`` statements.
- Create derived streams and tables from existing streams and tables with ``CREATE STREAM AS SELECT`` and
  ``CREATE TABLE AS SELECT`` statements.
- Convert data to different formats with ``CREATE STREAM AS SELECT`` and ``CREATE TABLE AS SELECT`` statements. For example,
  you can convert a stream from Avro to JSON.

Example KSQL Statements with Avro
=================================


Create a New Stream by Reading Avro-formatted Data
--------------------------------------------------

The following statement shows how to create a new ``pageviews`` stream by reading
from a Kafka topic that has Avro-formatted messages.

.. code:: sql

    CREATE STREAM pageviews
      WITH (KAFKA_TOPIC='pageviews-avro-topic',
            VALUE_FORMAT='AVRO');

Create a New Table by Reading Avro-formatted Data
-------------------------------------------------

The following statement shows how to create a new ``users`` table by reading from
a Kafka topic that has Avro-formatted messages.

.. code:: sql

   CREATE TABLE users
     WITH (KAFKA_TOPIC='users-avro-topic',
           VALUE_FORMAT='AVRO',
           KEY='userid');

In this example, you don’t need to define any columns or data types in the CREATE statement.
KSQL infers this information automatically from the latest registered Avro schema for the
``pageviews-avro-topic`` topic. KSQL uses the most recent schema at the time the statement
is first executed.

Create a New Stream with Selected Fields of Avro-formatted Data
---------------------------------------------------------------

If you want to create a STREAM or TABLE with only a subset of all the
available fields in the Avro schema, you must explicitly define the
columns and data types.

The following statement shows how to create a new ``pageviews_reduced`` stream,
which is similar to the previous example, but with only a few of the available
fields in the Avro data. In this example, only the ``viewtime`` and ``pageid``
columns are picked.

.. code:: sql

    CREATE STREAM pageviews_reduced (viewtime BIGINT, pageid VARCHAR)
      WITH (KAFKA_TOPIC='pageviews-avro-topic',
            VALUE_FORMAT='AVRO');

Convert a JSON Stream to an Avro Stream
---------------------------------------

KSQL allows you to work with streams and tables regardless of their underlying data format. This means that you can
easily mix and match streams and tables with different data formats and also convert between data formats. For
example, you can join a stream backed by Avro data with a table backed by JSON data.

In this example, only the ``VALUE_FORMAT`` is required for Avro to achieve the data conversion. KSQL automatically
generates an appropriate Avro schema for the new ``pageviews_avro`` stream, and it registers the schema with |sr|.

.. code:: sql

    CREATE STREAM pageviews_json (viewtime BIGINT, userid VARCHAR, pageid VARCHAR)
      WITH (KAFKA_TOPIC='pageviews-json-topic', VALUE_FORMAT='JSON');

    CREATE STREAM pageviews_avro
      WITH (VALUE_FORMAT = 'AVRO') AS
      SELECT * FROM pageviews_json;

For more information, see `Changing Data Serialization Format from JSON to Avro <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro>`__ 
in the `Stream Processing Cookbook <https://www.confluent.io/product/ksql/stream-processing-cookbook>`__.
