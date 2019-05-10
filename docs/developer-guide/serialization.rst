.. _ksql_serialization:

KSQL Serialization
==================

KSQL currently supports three serialization formats:

*. ``DELIMITED`` supports comma separated values. See :ref:`delimited_format` below.
*. ``JSON`` supports JSON values. See :ref:`json_format` below.
*. ``AVRO`` supports AVRO serialized values. See :ref:`avro_format` below.


.. _delimited_format

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

.. _json_format

----
JSON
----

The ``JSON`` format supports JSON values.

The JSON format supports all of KSQL's ref:`data types <data-types>`. As JSON does not itself
support a map type, KSQL serializes ``MAP``s as JSON objects.  Because of this the JSON format can
only support ``MAP`` objects that have ``STRING`` keys.

The serialized object should be a Kafka-serialized string containing a valid JSON value. The foramt
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

Top-level Primitives
--------------------

The JSON format supports reading top-level JSON primitives, but can if the target schema contains
a single field of a compatible type.

For example, given a KSQL statement with only a single field in the value schema:

.. code:: sql

    CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='JSON', ...);

And a JSON value of:

.. code:: json

       10

KSQL deserializes the JSON primitive ``10`` into the ``ID`` field of the stream.

However, if the value schema contains multiple fields, for example:

.. code:: sql

    CREATE STREAM x (ID BIGINT, NAME STRING) WITH (VALUE_FORMAT='JSON', ...);

Deserialization fails, because it's ambiguous as to which field the primitive value should be
deserialized into.

Top-level Arrays
----------------

The JSON format supports reading top-level JSON arrays, but only if the target schema contains a
single field of a compatible type.

For example, given a KSQL statement with only a single array field in the value schema:

.. code:: sql

    CREATE STREAM x (REGIONS ARRAY<STRING>) WITH (VALUE_FORMAT='JSON', ...);

And a JSON value of:

.. code:: json

       [
          "US",
          "EMEA"
       ]

KSQL deserializes the JSON array into the ``REGIONS`` field of the stream.

However, if the value schema contains multiple fields, for example:

.. code:: sql

    CREATE STREAM x (REGIONS ARRAY<STRING>, NAME STRING) WITH (VALUE_FORMAT='JSON', ...);

Deserialization fails, because it's ambiguous as to which field the primitive value should be
deserialized into.

Top-level Maps
--------------

.. tip:: When you deserialize JSON objects into a single ``MAP`` field, ensure the name of the
         field within the KSQL statement doesn't conflict with any of the keys in the map.
         Any conflict can lead to undesirable deserialization artifacts because KSQL treats the
         value as a normal JSON object, not as a map.

The JSON format supports reading a JSON object as a ``MAP``, but only if the target schema contains
a single field of a compatible type.

For example, given a KSQL statement with only a single map field in the value schema:

.. code:: sql

    CREATE STREAM x (PROPS MAP<STRING, STRING>) WITH (VALUE_FORMAT='JSON', ...);

And a JSON value of:

.. code:: json

       {
          "nodeCount": 10,
          "region": "us-12",
          "userId": "peter"
       }

KSQL deserializes the JSON map into the ``PROPS`` field of the stream.

However, if the value schema contains multiple fields, for example:

.. code:: sql

    CREATE STREAM x (PROPS MAP<STRING, STRING>, NAME STRING) WITH (VALUE_FORMAT='JSON', ...);

Deserialization fails, because it's ambiguous as to which field the primitive value should be
deserialized into.

A further potential ambiguity exists when working with top-level maps, when any of the keys of the
value match the name of the singular field in the target schema.

For example, given:

.. code:: sql

    CREATE STREAM x (PROPS MAP<STRING, STRING>) WITH (VALUE_FORMAT='JSON', ...);

And a JSON value of:

.. code:: json

       {
          "props": {
             "x": "y"
          },
          "region": "us-12",
          "userId": "peter"
       }

Deserializing the value is ambiguous: does KSQL deserialize to a top-level map or object? KSQL
deserializes the value as a JSON object, meaning ``PROPS`` is populated with an entry ``x -> y``
only.  Avoid this kind of ambiguity by ensuring the name of the field using in the KSQL statement
never clashes with a property name within the json object, or that the target schema contains more
than a single field.

.. _avro_format

----
Avro
----

The ``AVRO`` format supports Avro binary serialized of all of KSQL's ref:`data types <data-types>`
including records and top-level primitives, arrays and maps.

The format requires KSQL to be configured to store and retrieve the Avro schemas from the |sr-long|.
For more information, see :ref:`install_ksql-avro-schema`.

------------
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

-------------------------------------
Top-level primitives, arrays and maps
-------------------------------------

The Avro format supports reading top-level primitives, arrays and maps, but only if the target
schema contains a single field of a compatible type.

For example, given a KSQL statement with only a single field in the value schema:

.. code:: sql

    CREATE STREAM x (ID BIGINT) WITH (VALUE_FORMAT='JSON', ...);

And an Avro value serialized with the schema:

.. code:: json

       {
         { "type": "long" }
       }

KSQL can deserialize the values into the ``ID`` field of the stream.

However, if the value schema contains multiple fields, for example:

.. code:: sql

    CREATE STREAM x (ID BIGINT, NAME STRING) WITH (VALUE_FORMAT='JSON', ...);

Deserialization fails, because it's ambiguous as to which field the primitive value should be
deserialized into.
