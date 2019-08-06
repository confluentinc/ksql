.. _ksql_serialization:

KSQL Serialization
==================

.. contents:: :local:

=========================
Controlling serialization
=========================

KSQL offers several mechanisms for controlling serialization and deserialization.

The primary mechanism is by choosing the serialization format when you create
a stream or table and specify the ``VALUE_FORMAT`` in the ``WITH`` clause.

.. code:: sql

    CREATE TABLE ORDERS (F0 INT, F1 STRING) WITH (VALUE_FORMAT='JSON', ...);

For more information on the formats that KSQL supports, see :ref:`ksql_formats`.

KSQL provides some additional configuration that allows serialization to be controlled:

.. _ksql_single_field_wrapping

-------------------------
Single field (un)wrapping
-------------------------

.. note:: The ``DELIMITED`` and ``KAFKA`` formats do not support single field unwrapping.

Controlling deserializing of single fields
==========================================

When KSQL deserializes a Kafka message into a row, the key is deserialized into the key field,
and the message's value is deserialized into the value field(s).

By default, KSQL expects any value with a single-field schema to have been serialized as a named
field within a record. However, this is not always the case. KSQL also supports reading data
that has been serialized as an anonymous value.

For example, a value with multiple fields might look like the following in JSON:

.. code:: json

   {
      "id": 134,
      "name": "John"
   }

If the value only had the ``id`` field, KSQL would still expect the value to be serialized as a
named field, for example:

.. code:: json

   {
      "id": 134
   }

If your data contains only a single field, and that field is not wrapped within a JSON object,
or an Avro record if using the ``AVRO`` format, then you can use the ``WRAP_SINGLE_VALUE``
property in the ``WITH`` clause of your :ref:`CREATE TABLE <create-table>` or
:ref:`CREATE STREAM <create-stream>` statements. Setting the property to ``false`` tells KSQL
that the value is not wrapped, so the example above would be a JSON number:

.. code:: json

     134

For example, the following creates a table where the values in the underlying
topic have been serialized as an anonymous JSON number:

.. code:: sql

    CREATE TABLE TRADES (ID INT) WITH (WRAP_SINGLE_VALUE=false, ...);


If a statement doesn't set the value wrapping explicitly, KSQL uses the system
default, defined by ``ksql.persistence.wrap.single.values``. You can change the system default.
For more information, see :ref:`ksql-persistence-wrap-single-values`.

.. important:: KSQL treats ``null` keys and values as a special case. We recommend avoiding
               unwrapped single-field schemas if the field can have a ``null`` value.

A ``null`` value in a table's topic is treated as a tombstone, which indicates that a row has been
removed. If a table's source topic has an unwrapped single-field key schema and the value is
``null``, it's treated as a tombstone, resulting in any previous value for the key being removed
from the table.

A ``null`` key or value in a stream's topic is ignored when the stream is part of a join.
A ``null`` value in a table's topic is treated as a tombstone, and a ``null`` key is
ignored when the table is part of a join.

When you have an unwrapped single-field schema, ensure that any ``null`` key or
value has the desired result.

Controlling serialization of single fields
==========================================

When KSQL serializes a row into a Kafka message, the key field is serialized
into the message's key, and any value field(s) are serialized into the
message's value.

By default, if the value has only a single field, KSQL serializes the single field as a named field
within a record. However, this doesn't always match the requirements of downstream consumers,
so KSQL allows the value to be serialized as an anonymous value.

For example, consider the statements:

.. code:: sql

    CREATE STREAM x (f0 INT, f1 STRING) WITH (VALUE_FORMAT='JSON', ...);
    CREATE STREAM y AS SELECT f0 FROM x;

The second statement defines a stream with only a single field in the value,
named ``f0``.

By default, when KSQL writes out the result to Kafka, it persists the single field as
a named field within a JSON object, or an Avro record if using the ``AVRO`` format:

.. code:: json

   {
      "F0": 10
   }

If you require the value to be serialized as an anonymous value, for
example:

.. code:: json

   10

Then you can use the ``WRAP_SINGLE_VALUE`` property in your statement.

For example,

.. code:: sql

    CREATE STREAM y WITH(WRAP_SINGLE_VALUE=false) AS SELECT f0 FROM x;

If a statement doesn't set the value wrapping explicitly, KSQL uses the system
default, defined by ``ksql.persistence.wrap.single.values``. You can change the system default.
For more information, see :ref:`ksql-persistence-wrap-single-values`.

.. important:: KSQL treats ``null` keys and values as a special case. We recommended avoiding
               unwrapped single-field schemas if the field can have a ``null`` value.

A ``null`` value in a table's topic is treated as a tombstone, which indicates that a row has been
removed. If a table's source topic has an unwrapped single-field key schema and the value is
``null``, it's treated as a tombstone, resulting in any previous value for the key being removed
from the table.

A ``null`` key or value in a stream's topic is ignored when the stream is part of a join.
A ``null`` value in a table's topic is treated as a tombstone, and a ``null`` key is
ignored when the table is part of a join.

When you have an unwrapped single-field schema, ensure that any ``null`` key or
value has the desired result.

Single-field serialization examples
===================================

.. code:: sql

    -- Assuming system configuration is at the default:
    --  ksql.persistence.wrap.single.values=true

    -- creates a stream, picking up the system default of wrapping values.
    -- the serialized value is expected to be wrapped.
    -- if the serialized forms do not match the expected wrapping it will result in a deserialization error.
    CREATE STREAM IMPLICIT_SOURCE (NAME STRING) WITH (...);

    -- override 'ksql.persistence.wrap.single.values' to false
    -- the serialized value is expected to not be unwrapped.
    CREATE STREAM EXPLICIT_SOURCE (ID INT) WITH (WRAP_SINGLE_VALUE=false, ...);

    -- results in an error as the value schema is multi-field
    CREATE STREAM BAD_SOURCE (ID INT, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, ...);

    -- creates a stream, picking up the system default of wrapping values.
    -- the serialized values in the sink topic will be wrapped.
    CREATE STREAM IMPLICIT_SINK AS SELECT ID FROM S;

    -- override 'ksql.persistence.wrap.single.values' to false
    -- the serialized values will not be wrapped.
    CREATE STREAM EXPLICIT_SINK WITH(WRAP_SINGLE_VALUE=false) AS SELECT ID FROM S;

    -- results in an error as the value schema is multi-field
    CREATE STREAM BAD_SINK WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID, COST FROM S;

.. _ksql_formats:

=======
Formats
=======

KSQL currently supports three serialization formats:

* ``DELIMITED`` supports comma separated values. See :ref:`delimited_format` below.
* ``JSON`` supports JSON values. See :ref:`json_format` below.
* ``AVRO`` supports AVRO serialized values. See :ref:`avro_format` below.
* ``KAFKA`` supports primitives serialized using the standard Kafka serializers. See :ref:`kafka_format` below.

.. _delimited_format:

---------
DELIMITED
---------

The ``DELIMITED`` format supports comma separated values.

The serialized object should be a Kafka-serialized string, which will be split into columns.

For example, given a KSQL statement such as:

.. code:: sql

    CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='DELIMITED', ...);

KSQL splits a value of ``120, bob, 49`` into the three fields with ``ID`` of ``120``,
``NAME`` of ``bob`` and ``AGE`` of ``49``.

This data format supports all KSQL :ref:`data types <data-types>` except ``ARRAY``, ``MAP`` and
``STRUCT``.

.. _json_format:

----
JSON
----

The ``JSON`` format supports JSON values.

The JSON format supports all KSQL ref:`data types <data-types>`. As JSON doesn't itself
support a map type, KSQL serializes ``MAP``s as JSON objects.  Because of this the JSON format can
only support ``MAP`` objects that have ``STRING`` keys.

The serialized object should be a Kafka-serialized string containing a valid JSON value. The format
supports JSON objects and top-level primitives, arrays and maps. See below for more info.

JSON Objects
------------

Values that are JSON objects are probably the most common.

For example, given a KSQL statement such as:

.. code:: sql

    CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='JSON', ...);

And a JSON value of:

.. code:: json

       {
         "id": 120,
         "name": "bob",
         "age": "49"
       }

KSQL deserializes the JSON object's fields into the corresponding fields of the stream.

Top-level primitives, arrays and maps
-------------------------------------

The JSON format supports reading and writing top-level primitives, arrays and maps.

For example, given a KSQL statement with only a single field in the value schema and the
``WRAP_SINGLE_VALUE`` property set to ``false``:

.. code:: sql

    CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false, ...);

And a JSON value of:

.. code:: json

       10

KSQL can deserialize the values into the ``ID`` field of the stream.

When serializing data with a single field, KSQL can serialize the field as an anonymous value if
the ``WRAP_SINGLE_VALUE`` is set to ``false``, for example:

.. code:: sql

    CREATE STREAM y WITH (WRAP_SINGLE_VALUE=false) AS SELECT id FROM x;

For more information, see :ref:`ksql_single_field_wrapping`.

Field Name Case Sensitivity
---------------------------

The format is case-insensitive when matching a KSQL field name with a JSON document's property name.
The first case-insensitive match is used.

.. _avro_format:

----
Avro
----

The ``AVRO`` format supports Avro binary serialization of all KSQL ref:`data types <data-types>`,
including records and top-level primitives, arrays, and maps.

The format requires KSQL to be configured to store and retrieve the Avro schemas from the |sr-long|.
For more information, see :ref:`install_ksql-avro-schema`.

Avro Records
------------

Avro records can be deserialized into matching KSQL schemas.

For example, given a KSQL statement such as:

.. code:: sql

    CREATE STREAM x (ID BIGINT, NAME STRING, AGE INT) WITH (VALUE_FORMAT='JSON', ...);

And an Avro record serialized with the schema:

.. code:: json

       {
         "type": "record",
         "namespace": "com.acme",
         "name": "UserDetails",
         "fields": [
           { "name": "id", "type": "long" },
           { "name": "name", "type": "string" }
           { "name": "age", "type": "int" }
         ]
       }

KSQL deserializes the Avro record's fields into the corresponding fields of the stream.

Top-level primitives, arrays and maps
-------------------------------------

The Avro format supports reading and writing top-level primitives, arrays and maps.

For example, given a KSQL statement with only a single field in the value schema and the
``WRAP_SINGLE_VALUE`` property set to ``false``:

.. code:: sql

    CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='AVRO', WRAP_SINGLE_VALUE=false, ...);

And an Avro value serialized with the schema:

.. code:: json

       {
         "type": "long"
       }

KSQL can deserialize the values into the ``ID`` field of the stream.

When serializing data with a single field, KSQL can serialize the field as an anonymous value if
the ``WRAP_SINGLE_VALUE`` is set to ``false``, for example:

.. code:: sql

    CREATE STREAM y WITH (WRAP_SINGLE_VALUE=false) AS SELECT id FROM x;

For more information, see :ref:`ksql_single_field_wrapping`.

Field Name Case Sensitivity
---------------------------

The format is case-insensitive when matching a KSQL field name with an Avro record's field name.
The first case-insensitive match is used.

.. _kafka_format

-----
KAFKA
-----

The ``KAFKA`` format supports``INT``, ``BIGINT``, ``DOUBLE`` and ``STRING`` primitives that have
been serialized using Kafka's standard set of serializers.

The format is designed primarily to support primitive message keys. It can be used as a value format,
though certain operations aren't supported when this is the case.

Unlike some other formats, the ``KAFKA`` format does not perform any type coercion, so it's important
to correctly match the field type to the underlying serialized form to avoid deserialization errors.

The table below details the SQL types the format supports, including details of the associated Kafka
Java Serializer, Deserializer and Connect Converter classes you would need to use to write the key
to Kafka, read the key from Kafka, or use to configure Apache Connect to work with the ``KAFKA`` format,
respectively.

+------------------+--------------------------------+-------------------------------------------------------------+---------------------------------------------------------------+-------------------------------------------------------+
| KSQL Field Type  | Kafka Type                     | Kafka Serializer                                            | Kafka Deserializer                                            | Connect Converter                                     |
+==================+================================+=============================================================+===============================================================+=======================================================+
| INT / INTEGER    | A 32-bit signed integer        | ``org.apache.kafka.common.serialization.IntegerSerializer`` | ``org.apache.kafka.common.serialization.IntegerDeserializer`` | ``org.apache.kafka.connect.storage.IntegerConverter`` |
+------------------+--------------------------------+-------------------------------------------------------------+---------------------------------------------------------------+-------------------------------------------------------+
| BIGINT           | A 64-bit signed integer        | ``org.apache.kafka.common.serialization.LongSerializer``    | ``org.apache.kafka.common.serialization.LongDeserializer``    | ``org.apache.kafka.connect.storage.LongConverter``    |
+------------------+--------------------------------+-------------------------------------------------------------+---------------------------------------------------------------+-------------------------------------------------------+
| DOUBLE           | A 64-bit floating point number | ``org.apache.kafka.common.serialization.DoubleSerializer``  |``org.apache.kafka.common.serialization.DoubleDeserializer``   | ``org.apache.kafka.connect.storage.DoubleConverter``  |
+------------------+--------------------------------+-------------------------------------------------------------+---------------------------------------------------------------+-------------------------------------------------------+
| STRING / VARCHAR | A UTF-8 encoded text string    | ``org.apache.kafka.common.serialization.StringSerializer``  |``org.apache.kafka.common.serialization.StringDeserializer``   | ``org.apache.kafka.connect.storage.StringConverter``  |
+------------------+--------------------------------+-------------------------------------------------------------+---------------------------------------------------------------+-------------------------------------------------------+

Because the format supports only primitive types, you can only use it when the schema contains a single field.

For example, if your Kafka messages have a ``long`` key, you can make them available to KSQL a statement
similar to:

.. code:: sql

    CREATE STREAM USERS (ROWKEY BIGINT KEY, NAME STRING) WITH (KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', ...);
