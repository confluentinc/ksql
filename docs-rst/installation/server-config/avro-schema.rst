.. _install_ksql-avro-schema:

Avro and Schema Registry
========================

KSQL can read and write messages in Avro format by integrating with :ref:`Confluent Schema Registry <schemaregistry_intro>`.
KSQL automatically retrieves (read) and registers (write) Avro schemas as needed and thus saves you from both having to
manually define columns and data types in KSQL and from manual interaction with the Schema Registry.

.. contents:: Contents
    :local:

Supported functionality
^^^^^^^^^^^^^^^^^^^^^^^

KSQL currently supports Avro data in the Kafka message values.

The following functionality is not supported yet:

-  Message keys in Avro format are not supported. Message keys in KSQL are always interpreted as STRING format, which means
   KSQL will ignore Avro schemas that have been registered for message keys and the key will be read using StringDeserializer.
-  Avro schemas with nested fields are not supported yet. This is because KSQL does not yet support nested columns. This
   functionality is coming soon.

Configuring KSQL for Avro
^^^^^^^^^^^^^^^^^^^^^^^^^

You must configure the REST endpoint of |sr| by setting ``ksql.schema.registry.url`` (default: ``http://localhost:8081``)
in the KSQL server configuration file (``<path-to-confluent>/etc/ksql/ksql-server.properties``). For more information,
see :ref:`install_ksql-server`.

.. important:: Do not use the SET statement in the KSQL CLI to configure the registry endpoint.

Using Avro in KSQL
^^^^^^^^^^^^^^^^^^

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

Example: Create a new stream ``pageviews`` by reading from a Kafka topic with Avro-formatted messages.
    .. code:: sql

        CREATE STREAM pageviews
          WITH (KAFKA_TOPIC='pageviews-avro-topic',
                VALUE_FORMAT='AVRO');

Example: Create a new table ``users`` by reading from a Kafka topic with Avro-formatted messages.
    In this example you don’t need to define any columns or data types in the CREATE statement because KSQL automatically
    infers this information from the latest registered Avro schema for topic ``pageviews-avro-topic`` (i.e., the latest
    schema at the time the statement is first executed).

    .. code:: sql

        CREATE TABLE users
          WITH (KAFKA_TOPIC='users-avro-topic',
                VALUE_FORMAT='AVRO',
                KEY='userid');



    If you want to create a STREAM or TABLE with only a subset of all the
    available fields in the Avro schema, then you must explicitly define the
    columns and data types.

Example: Create a new stream ``pageviews_reduced``
    Similar to the previous example, but with only a few of all the available fields in the Avro data. In this example,
    only the ``viewtime`` and ``pageid`` columns are picked).

    .. code:: sql

        CREATE STREAM pageviews_reduced (viewtime BIGINT, pageid VARCHAR)
          WITH (KAFKA_TOPIC='pageviews-avro-topic',
                VALUE_FORMAT='AVRO');

    KSQL allows you to work with streams and tables regardless of their underlying data format. This means that you can
    easily mix and match streams and tables with different data formats and also convert between data formats. For
    example, you can join a stream backed by Avro data with a table backed by JSON data.

Example: Convert a JSON stream into an Avro stream.
    In this example only the ``VALUE_FORMAT`` is required for Avro to achieve the data conversion. KSQL automatically
    generates an appropriate Avro schema for the new ``pageviews_avro`` stream, and it  registers the schema with |sr|.

    .. code:: sql

        CREATE STREAM pageviews_json (viewtime BIGINT, userid VARCHAR, pageid VARCHAR)
          WITH (KAFKA_TOPIC='pageviews-json-topic', VALUE_FORMAT='JSON');

        CREATE STREAM pageviews_avro
          WITH (VALUE_FORMAT = 'AVRO') AS
          SELECT * FROM pageviews_json;



    