# KLIP 3 - Deserialization and Serialization of Single Fields.

**Author**: @big-andy-coates | 
**Release Target**: 5.3 + 1 | 
**Status**: values: Merged, keys: Design Approved | 
**Discussion**: [PR #2824](https://github.com/confluentinc/ksql/pull/2824)

**tl;dr:** _Add the ability to deserialize from, and serialize to, primitive types, 
arrays and maps._

## Terminology

Both JSON and AVRO support serialization of objects with multiple named fields.
JSON has _JSON objects_, while Avro has _Avro records_.

Both formats also support other types: JSON supports an array type and a small 
selection of primitive types (`Number`, `String`, and `Boolean`); Avro supports
array and map types along with a wider range of primitive types.

The collective term _record_ is used to refer to JSON _objects_ and Avro _records_. 
The term _non-record_ is used to refer to any other types.

A row in KSQL has a logical schema made up of multiple fields. Fields within the row's
schema are persisted to either the key or value of the underlying Kafka record. 
In this sense the key and value also have a logical schema.  

The term _single field schema_ is used to describe a key or value logical schema where 
the schema contains only a single field. 

The term _top-level_ is used to describe the first entity found within serialized data,
for example a 'top-level record' would mean the serialized data contains a record
with named fields, where as a 'top-level numnber' would mean the data contains a
JSON or Avro number, with no wrapping _record_.
           
## Motivation and background

Currently, the `JSON` and `AVRO` formats in KSQL can only deserialize _record_ values, 
i.e. those in the form of a JSON object or an Avro record. These formats can 
only deserialize _non-record_ types if those type are contained as a field within a
top-level _record_ type.

Likewise, KSQL always serializes the value fields with in a schema as fields within
a top-level _record_. There is no support for serializing schemas that contain only 
a single value field as _non-record_ types. For example, if a query selects only a 
single `INT` value field, it will be serialized within a _record_, it can not be 
serialized as a simple integer.

On the other hand, KSQL only supports string keys, i.e. a _non-record_ type.

Being able to both process and produce both _record_ and _non-record_ values opens up 
KSQL to more use cases, and is a precondition of enhancing KSQL to support _record_
keys, a.k.a. _structured keys_, a.k.a _composite keys_. 
(Support for _non_record_ keys will be needed to allow KSQL to maintain 
backwards compatibility with existing queries, which have string keys).

For example, given a statement such as: 

```
CREATE STREAM USERS (NAME STRING) WITH (...);
 ``` 
 
The logical value schema has only a single field. KSQL should be able to read 
data in Kafka where the value is a anonymous top-level JSON or Avro string, rather 
than only a _record_ containing a `NAME` field.

Given a statement such as:

```
CREATE STREAM FOO AS SELECT ID FROM BAR;
 ``` 
 
The logical value schema is again only a single field, lets say of type `INT`. 
Users should be able to control if this is serialized as a named `ID` field within
_record_, or as an anonymous JSON number or Avro int. 
 

## What is in scope

* JSON (de)serialization of schemas containing a single value field that 
  is of SQL type `ARRAY`, `MAP`, `STRUCT` or a primitive type.
  
* Avro (de)serialization of schemas containing a single value field that
  is of SQL type `ARRAY`, `MAP`, `STRUCT` or a primitive type.  
  
* (De)serialization of _record_ and _non-record_ keys, but only in the context on
  ensuring a design that can be extended to include such keys.
  
* Any configuration and/or syntax used to control the (de)serialization of 
_single field schemas_.

* Maintaining compatibility with queries started on earlier versions of KSQL,
  i.e. ensuring those queries continue to serialize single field schemas as
  they did previously.

## What is not in scope

* Extensive details of how KSQL will be extended to support all _record_ and 
  _non-record_ key types. This is future work. 
  
* Other current or future formats. Though the design needs to be mindful to 
  ensure functionality is compatible with future formats that KSQL may support.

## Value/Return

The work is predominantly about paving the way for the structured key work,
as this KLIP is a precondition of said work.

However, this functionality will allow KSQL to work with more _value_ schemas, 
opening up KSQL to more use-cases.

## Public APIS

### New config:

* `ksql.persistence.wrap.single.values`: which provides a default for how 
  VALUE schemas with a single field should be deserialized and serialized:
  `true` indicating values should be persisted within a _record_; `false`
  indicating values should be persisted as anonymous values.
  
  The default value will be `true`  i.e. values will be wrapped by default.
  The drivers for this choice are:
  
  1. Changing a query with a single-field schema to include another field 
  in the value is an evolvable change if the value is wrapped, but a breaking
  change if the value is unwrapped.
  1. A default of `true` gives us backwards compatibility with old queries,
  which wrap single-field schemas, for free.
  
  This setting will affect both C* and C*\AS statements if no explicit
  override is provided.
  
* `ksql.persistence.wrap.single.keys`: which provides a default for how 
  KEY schemas with a single field should be deserialized and serialized:
  `true` indicating keys should be persisted within a _record_; `false`
  indicating values should be persisted as anonymous values.
  
  The default value will be `false`, i.e. keys will be unwrapped by default.
  The drivers for this choice are:
  
  1. This is inline with how many people will use KSQL. A simple primitive key
  is very common.
  1. Changing a query with a single-field schema to include another field in 
  the key would be a breaking change even if the key was already a record, 
  as it will change partitioning.
  1. A default of `false` gives us backwards compatibility with old queries, 
  which have string keys, for free.
  
  This setting will affect both C* and C*\AS statements if no explicit
  override is provided.
  
  Note: this new config will be added as part of the future structured 
  keys work.
 
### SQL syntax

Users can override the configured defaults and control how single-field
keys and values schemas are serialized by providing the following `WITH` 
clause properties in either C* or C\*AS statement.

* `WRAP_SINGLE_VALUE`: boolean property that will override the
  `ksql.persistence.wrap.single.values` configuration.

* `WRAP_SINGLE_KEY`: boolean property that will override the
  `ksql.persistence.wrap.single.keys` configuration.
  
  Note: to be added as part of the future structured keys work.
  
* `WRAP_SINGLE_FIELDS`: boolean property that is a short hand for 
  setting both `WRAP_SINGLE_KEY` and `WRAP_SINGLE_VALUE`.
  
  Note: to be added as part of the future structured keys work.

## Design

### `CREATE STREAM` and `CREATE TABLE`

C* statements will, by default, capture the values of 
`ksql.persistence.wrap.single.keys` and `ksql.persistence.wrap.single.values`.

Users can override these settings by providing any of 
the `WRAP_SINGLE_XXX` family of `WITH` clause properties.

These settings will be stored as part of the created source's metadata. 

These settings control how the data in the source's topic should be
deserialized by downstream queries.

Providing any of these settings will cause an error to be returned should 
the associated schema(s) be multi-field.   

For example:

```sql
-- Default config: ksql.persistence.wrap.single.values=false

-- creates a stream, picking up the system default of not wrapping
-- the serialized value is expected to not be wrapped. 
-- if the serialized value is wrapped it will likely result in a deserialization error.
CREATE STREAM IMPLICIT_SOURCE (ID INT) WITH (...);

-- override 'ksql.persistence.wrap.single.values' to true
-- the serialized value is expected to not be unwrapped.
CREATE STREAM EXPLICIT_SOURCE (ID INT) WITH (WRAP_SINGLE_VALUE=true, ...); 

-- results in an error as the value schema is multi-field
CREATE STREAM BAD_SOURCE (ID INT, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, ...);

-- [In future, with structured keys]
-- override both settings to false.
-- indicating that the key and value data in the topic are not wrapped
CREATE TABLE SK_SOURCE (ID INT KEY, NAME STRING) WITH (WRAP_SINGLE_FIELDS=false, ...); 
``` 

### `CREATE STREAM AS` and `CREATE TABLE AS`

C\*AS statements, by default, capture the values of 
`ksql.persistence.wrap.single.keys` and `ksql.persistence.wrap.single.values`.

Users can override these settings by providing any of 
the `WRAP_SINGLE_XXX` family of `WITH` clause properties.

These settings will be stored as part of the created source's metadata. 

These settings control how the data in the source's topic should be
serialized by this query and deserialized by any downstream queries.

Providing any of these settings will cause an error to be returned should 
the associated schema(s) be multi-field.    

For example:

```sql
-- Default config: ksql.persistence.wrap.single.values=false

-- creates a stream, picking up the system default of not wrapping
-- the serialized values in the underlying topic will not be wrapped. 
CREATE STREAM IMPLICIT_SOURCE AS SELECT ID FROM S;

-- override 'ksql.persistence.wrap.single.values' to true
-- the serialized values will be wrapped.
CREATE STREAM EXPLICIT_SOURCE WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID FROM S;

-- results in an error as the value schema is multi-field
CREATE STREAM BAD_SOURCE WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID, COST FROM S;

-- [In future, with structured keys]
-- explicitly setting both settings to false.
-- the serialized keys and values will be unwrapped.
CREATE SOURCE SK_SOURCE WITH(WRAP_SINGLE_FIELDS=false) AS SELECT ID KEY, NAME STRING FROM S;
 ```
 
### INSERT

Insert statements do not create sources of their own. 
They use the serialization settings of their sink.

For example,

```sql
-- will use the serialization settings of 'SINK` to determine if value should be wrapped or not.
INSERT INTO SINK SELECT ID FROM SOURCE;

-- will also use the serialization settings of `SINK`, though in this case they will be wrapped as its multi-field.
INSERT INTO SINK SELECT ID, NAME FROM SOURCE;
 ```
 
### INSERT VALUES

Insert values statements do not create sources of their own.
They use the serialization settings of their sink.
 
```sql
-- will use the serialization settings of 'SINK` to determine if single field schemas should be wrapped or not.
INSERT INTO SINK (ID) VALUES (10);

-- will also use the serialization settings of `SINK`, though in this case they will be wrapped as its multi-field.
INSERT INTO SINK VALUES (10, 'bob');
 ```
 

## Test plan

Tests will be added to cover all valid combinations and permutations of 
the following dimensions:

* Format: `JSON` and `AVRO`
* `ksql.persistence.wrap.single.keys` config?: `true` and `false` 
* `ksql.persistence.wrap.single.values` config?: `true` and `false`
* Source type: `TABLE` and `STREAM`
* Source key: unwrapped single field, wrapped single field, multiple fields
* Source value: unwrapped single field, wrapped single field, multiple fields
* Source `WRAP_SINGLE_KEY`: `true` and `false` 
* Source `WRAP_SINGLE_VALUE`: `true` and `false` 
* Source `WRAP_SINGLE_FIELDS`: `true` and `false` 
* Query type: `TABLE` and `STREAM`
* Query `WRAP_SINGLE_KEY`: `true` and `false` 
* Query `WRAP_SINGLE_VALUE`: `true` and `false` 
* Query `WRAP_SINGLE_FIELDS`: `true` and `false`
* Query key schema: single and multiple fields
* Query value schema: single and multiple fields

This will initially be done only for values. 
Key support will be added as part of the structured keys work.

Note: combining `WRAP_SINGLE_FIELDS` with either `WRAP_SINGLE_KEY` or
`WRAP_SINGLE_VALUE` should result in a duplicate setting error.

JSON tests will also be added to ensure the new configurations can be
set via the `SET` command and are picked as expected.

Existing tests are sufficient to ensure these changes do not effect the 
format or schema of internal topics.

## Documentation Updates

* The server configuration documentation in `config-reference.rst` will 
have the following two settings added:

```rst
.. _ksql-persistence-wrap-single-keys:

---------------------------------
ksql.persistence.wrap.single.keys
---------------------------------

Sets the default value for the ``WRAP_SINGLE_KEY`` property if one is
not supplied explicitly in :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE AS SELECT <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

This setting can be toggled using the `SET` command

.. code:: sql

    SET 'ksql.persistence.wrap.single.keys'='true';

For more information, refer to the :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE AS SELECT <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

.. note:: This setting has no effect on formats that do not support some 
          kind of outer record or object. For example, ``DELIMITED``.

.. _ksql-persistence-wrap-single-values:

-----------------------------------
ksql.persistence.wrap.single.values
-----------------------------------

Sets the default value for the ``WRAP_SINGLE_VALUE`` property if one is
not supplied explicitly in :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE AS SELECT <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

This setting can be toggled using the `SET` command

.. code:: sql

    SET 'ksql.persistence.wrap.single.values'='false';

For more information, refer to the :ref:`CREATE TABLE <create-table>`,
:ref:`CREATE STREAM <create-stream>`, :ref:`CREATE TABLE AS SELECT <create-table-as-select>`
or :ref:`CREATE STREAM AS SELECT <create-stream-as-select>` statements.

.. note:: This setting has no effect on formats that do not support some 
          kind of outer record or object. For example, ``DELIMITED``.
```

* The `CREATE TABLE` and `CREATE STREAM` sections in `syntax-reference.rst`
  will have an updated properties section that includes the following rows:

```rst
 +=========================+========================================================================================================+
 | WRAP_SINGLE_KEY         | Controls how keys are deserialized where the key's schema contains only a single field.                |
 |                         |                                                                                                        |
 |                         | The setting controls how KSQL will deserialize the key of the records in the supplied ``KAFKA_TOPIC``. |
 |                         | If set to ``true`` KSQL expects the field(s) to have been serialized as named field(s) within a record.|
 |                         | If set to ``false`` and the key has a single-field schema, KSQL expects the field to have been         |
 |                         | serialized as an anonymous value.                                                                      |
 |                         | Setting to ``false`` when the key is a multi-field schema will result in an error                      |
 |                         |                                                                                                        |
 |                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-keys` and           |
 |                         | defaulting to ``false``, is used.                                                                      |
 |                         |                                                                                                        |
 |                         | Note: setting this property on formats that do not support wrapping, for example `DELIMITED`,          |
 |                         | will result in an error                                                                                |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 | WRAP_SINGLE_VALUE       | Controls how values are deserialized where the value's schema contains only a single field.            |
 |                         |                                                                                                        |
 |                         | The setting controls how KSQL will deserialize the value of the records in the supplied ``KAFKA_TOPIC``.|
 |                         | If set to ``true`` KSQL expects the field(s) to have been serialized as named field(s) within a record.|
 |                         | If set to ``false`` and the value has a single-field schema, KSQL expects the field to have been       |
 |                         | serialized as an anonymous value.                                                                      |
 |                         | Setting to ``false`` when the value is a multi-field schema will result in an error                    |
 |                         |                                                                                                        |
 |                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` and         |
 |                         | defaulting to ``true``, is used.                                                                      |
 |                         |                                                                                                        |
 |                         | Note: setting this property on formats that do not support wrapping, for example `DELIMITED`,          |
 |                         | will result in an error                                                                                |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 | WRAP_SINGLE_FIELDS      | Shorthand for setting both ``WRAP_SINGLE_KEY`` and ``WRAP_SINGLE_VALUE`` to the same value.            |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 ```

* The `CREATE TABLE AS SELECT` and `CREATE STREAM AS SELECT` sections in `syntax-reference.rst`
  will have an updated properties section that includes the following rows:

```rst
 +=========================+========================================================================================================+
 | WRAP_SINGLE_KEY         | Controls how keys are serialized where the key's schema contains only a single field.                  |
 |                         |                                                                                                        |
 |                         | The setting controls how the query will serialize keys with a single-field schema.                     |
 |                         | If set to ``true`, KSQL will serialize field as a named field within a record.                         |
 |                         | If set to ``false`` KSQL, and the key has a single-field schema, KSQL will serialize the field as an   |
 |                         | anonymous anonymous value.                                                                             |
 |                         | Setting to ``false`` when the key is a multi-field schema will result in an error                      |
 |                         |                                                                                                        |
 |                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-keys` and           |
 |                         | defaulting to ``false``, is used.                                                                      |                                                                                                      |
 |                         |                                                                                                        |
 |                         | Note: setting this property on formats that do not support wrapping, for example `DELIMITED`,          |
 |                         | will result in an error                                                                                |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 | WRAP_SINGLE_VALUE       | Controls how values are serialized where the value's schema contains only a single field.              |
 |                         |                                                                                                        |
 |                         | The setting controls how the query will serialize values with a single-field schema.                   |
 |                         | If set to ``true`, KSQL will serialize field as a named field within a record.                         |
 |                         | If set to ``false`` KSQL, and the value has a single-field schema, KSQL will serialize the field as an |
 |                         | anonymous anonymous value.                                                                             |
 |                         | Setting to ``false`` when the value is a multi-field schema will result in an error                    |
 |                         |                                                                                                        |
 |                         | If not supplied, the system default, defined by :ref:`ksql-persistence-wrap-single-values` and         |
 |                         | defaulting to ``true``, is used.                                                                      |
 |                         |                                                                                                        |
 |                         | Note: setting this property on formats that do not support wrapping, for example `DELIMITED`,          |
 |                         | will result in an error                                                                                |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 | WRAP_SINGLE_FIELDS      | Shorthand for setting both ``WRAP_SINGLE_KEY`` and ``WRAP_SINGLE_VALUE`` to the same value.            |
 +-------------------------+--------------------------------------------------------------------------------------------------------+
 ```

* A new ``serialization.rst`` will be added to the developer guide:

```rst
_ksql_serialization:

KSQL Serialization
==================

.. _ksql_formats

=======
Formats
=======

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

This data format supports all KSQL :ref:`data types <data-types>` except for container types, 
i.e. ``ARRAY``, ``MAP`` and ``STRUCT``.

.. note:: As the ``DELIMITED`` format has no concept of wrapping serialized data in some form
          of object or record it does not support the ``WRAP_SINGLE_XXX` family of properties
          and is not affected by the underlying ``ksql.persistence.wrap.single.xxx` system
          configurations.  

.. _json_format

----
JSON
----

The ``JSON`` format supports JSON values.

The JSON format supports all of KSQL's ref:`data types <data-types>`. As JSON does not itself
support a map type, KSQL serializes ``MAP``s as JSON objects.  Because of this, the JSON format
can only support ``MAP`` objects that have ``STRING`` keys.

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

KSQL will deserialize the JSON object's fields into the corresponding fields of the stream.

.. note:: KSQL has special handling of single-field key and value schemas. KSQL supports
          single-fields schemas being serialized as a named field within a JSON object or
          an anonymous JSON value. By default, KSQL expects source data to have single-field
          keys to have been serialized as anonymous values, and values as JSON objects.
          Likewise, KSQL defaults to serializing output data the same way. KSQL's
          handling of single-field schemas can be controlled. For more information, 
          see :ref:`single_field_wrapping`.
          
-------------------------------------
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
    
Serialization of single-field key schemas can similarly be controlled via the ``WRAP_SINGLE_KEY``
property. Note however that the default for single-field key schemas is anonymous values.

For more information, see :ref:`ksql_single_field_wrapping`.

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

KSQL will deserialize the Avro record's fields into the corresponding fields of the stream.

.. note:: KSQL has special handling of single-field key and value schemas. KSQL supports
          single-fields schemas being serialized as a named field within an Avro record or
          an anonymous Avro value. By default, KSQL expects source data to have single-field
          keys to have been serialized as anonymous values, and values as Avro records.
          Likewise, KSQL defaults to serializing output data the same way. KSQL's
          handling of single-field schemas can be controlled. For more information, 
          see :ref:`single_field_wrapping`.

-------------------------------------
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
    
Serialization of single-field key schemas can similarly be controlled via the ``WRAP_SINGLE_KEY``
property. Note however that the default for single-field key schemas is anonymous values.

For more information, see :ref:`ksql_single_field_wrapping`.

=========================
Controlling serialization
=========================

KSQL offers several mechanisms for controlling serialization and deserialization.

The primary mechanism is by choosing the serialization format when you create
a stream or table and specify the ``VALUE_FORMAT`` in the ``WITH`` clause.

.. code:: sql

    CREATE TABLE x (F0 INT, F1 STRING) WITH (VALUE_FORMAT='JSON', ...);

For more information on the formats that KSQL supports, see :ref:`ksql_formats`.

KSQL provides some additional configuration that allows serialization to be controlled:

.. _single_field_wrapping
-------------------------
Single field (un)wrapping
-------------------------

.. note:: The ``DELIMITED`` format is not affected by single field unwrapping.


Controlling deserializing of single fields
==========================================

When KSQL deserializes a Kafka record into a row, the key is deserialized into
the key field(s) and the record's value is deserialized into the value field(s).

By default, KSQL expects any key with a single-field schema to have been 
serialized as an anonymous value, not as a named field within a record, as 
would be the case if there were multiple fields. 

Conversely, by default, KSQL expects any value with a single-field schema 
to have been serialized as a named field within a record.  

The defaults for single-field key and values differ as they reflect the
most common usages of KSQL. Also, any change to the key schema is likely
a breaking change for any existing system, as it will change the partitioning
of data. Therefore, evolution of key schemas is generally not a concern.

Taking the value schema as an example, a value with multiple fields might
look like the following in JSON: 

.. code:: json

   {
      "id": 134,
      "name": "John"
   }
   
If the value only had the ``id`` field, KSQL would still expect the value
to be serialized as a named field, for example:

.. code:: json

   {
      "id": 134
   }
  
If your data contains only a single field, and that field is not wrapped 
within a JSON object, (or an Avro record if using the ``AVRO`` format), then
you can use the ``WRAP_SINGLE_VALUE`` property in the ``WITH`` clause of
your :ref:`CREATE TABLE <create-table>` or :ref:`CREATE STREAM <create-stream>`
statements. Setting the property to ``false`` will inform KSQL that the 
value is not wrapped, i.e. the example above would be a JSON number:

.. code:: json

     134
     
For example, the following creates a table where the values in the underlying
topic have been serialized as an anonymous JSON number:

.. code:: sql

    CREATE TABLE x (ID INT) WITH (WRAP_SINGLE_VALUE=false, ...);


KSQL also supports ``WRAP_SINGLE_KEY`` to control key deserialization in
:ref:`CREATE TABLE <create-table>` or :ref:`CREATE STREAM <create-stream>`
statements, and a ``WRAP_SINGLE_FIELDS``, which is a shorthand for setting
both properties.
 
If a statement does not explicitly set the key or value wrapping, the system
defaults, defined by ``ksql.persistence.wrap.single.keys`` and
``ksql.persistence.wrap.single.values``, will be used. These system defaults 
can also be changed. For more information, see the 
:ref:`ksql-persistence-wrap-single-keys` and 
:ref:`ksql-persistence-wrap-single-values` documentation. 

Controlling serialization of single fields
==========================================

When KSQL serializes a row into a Kafka record, the key field(s) are serialized 
into the record's key, and any value field(s) are serialized into the 
record's value. 

By default, if the key has only a single field, KSQL will serialize the 
single field as an anonymous value, rather than as a named field within a 
record, as it would if there were multiple fields.

Conversely, if the value has only a single field, KSQL will serialize the
single field as a named field within a record.

The defaults for single-field key and values differ as they reflect the
most common usages of KSQL. Also, any change to the key schema is likely
a breaking change for any existing system, as it will change the partitioning
of data. Therefore, evolution of key schemas is generally not a concern.

Taking the value schema as an example, consider the statements:

.. code:: sql

    CREATE STREAM x (f0 INT, f1 STRING) WITH (VALUE_FORMAT='JSON', ...);
    CREATE STREAM y AS SELECT f0 FROM x;

The second statement defines a stream with only a single field in the value, 
named ``f0``. 

When KSQL writes out the result to Kafka it will, by default, persist the 
single field as a named field within a JSON object, (or an Avro record if 
using the ``AVRO`` format):

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
    
Likewise, the ``WRAP_SINGLE_KEY`` property can be used to control the 
serialization of keys with single fields, or you can use 
``WRAP_SINGLE_FIELDS`` as shorthand for setting both.

If a statement does not explicitly set the key or value wrapping, the system
defaults, defined by ``ksql.persistence.wrap.single.keys`` and
``ksql.persistence.wrap.single.values``, will be used. These system defaults 
can also be changed. For more information, see the 
:ref:`ksql-persistence-wrap-single-keys` and 
:ref:`ksql-persistence-wrap-single-values` documentation. 

Examples
==========================================

.. code:: sql

    -- Assuming system configuration is at the default:
    --  ksql.persistence.wrap.single.keys=false
    --  ksql.persistence.wrap.single.values=true

    -- creates a stream, picking up the system default of not wrapping keys and wrapping values.
    -- the serialized key is expected to not be wrapped. 
    -- the serialized value is expected to be wrapped. 
    -- if the serialized forms do not match the expected wrapping it will result in a deserialization error.
    CREATE STREAM IMPLICIT_SOURCE (ID INT KEY, NAME STRING) WITH (...);
    
    -- override 'ksql.persistence.wrap.single.values' to false
    -- the serialized value is expected to not be unwrapped.
    CREATE STREAM EXPLICIT_SOURCE (ID INT) WITH (WRAP_SINGLE_VALUE=false, ...); 
    
    -- results in an error as the value schema is multi-field
    CREATE STREAM BAD_SOURCE (ID INT, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, ...);

    -- creates a stream, picking up the system default of not wrapping keys and wrapping values.
    -- the serialized keys in the sink topic will not be wrapped. 
    -- the serialized values in the sink topic will be wrapped. 
    CREATE STREAM IMPLICIT_SINK AS SELECT ID FROM S;
    
    -- override 'ksql.persistence.wrap.single.keys' to true
    -- the serialized keys and values will be wrapped.
    CREATE STREAM EXPLICIT_SINK WITH(WRAP_SINGLE_KEY=true) AS SELECT ID FROM S;
    
    -- results in an error as the value schema is multi-field
    CREATE STREAM BAD_SINK WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID, COST FROM S;
```

* Suitable details will be added to the `changelog.rst`.

# Compatibility Implications

Tests are already inplace to ensure this change does not change the schema 
or format of any internal topics.

Legacy queries, i.e. those started on previous versions of KSQL, have
unwrapped string keys and wrap values with single field schemas.
To maintain backwards compatibility this must continue to be the case _after_
the proposed work is complete.

Backwards compatibility of the value schema is maintained as the default
for `ksql.persistence.wrap.single.values` is `true`. Meaning legacy queries
will continue to wrap single-field value schemas.

Backwards compatibility of key schemas is maintained as the default
for `ksql.persistence.wrap.single.keys` is `false`. Meaning legacy queries
will continue to not wrap single-field key schemas. 

## Performance Implications

The decision to deserialize and serialize values either wrapped or unwrapped is done only when
initiating a query. From then on the serde classes are working with a fixed schema. Therefore
there should be no performance implications outside of the cost of serializing and deserializing
the wrapper _record_ if the user has configured the query to use them.

## Security Implications

None.

# Rejected alternatives

## Smart deserializers

Rather than have the deserialization of wrapped vs unwrapped single field schemas be controlled 
by the ``WRAP_SINGLE_XXX`` family of properties, it is _almost_ possible to have the 
deserializers able to determine _at runtime_ and on a _record by record_ basis whether
the serialized data contains an anonymous value or the single field within a record.

For example, consider

```sql
CREATE STREAM FOO (ID INT) WITH (...);
```

The logical value schema contains a single `ID` integer field.

The JSON deserializer would be able to handle deserialize both of the following JSON values:

```json
{
   "ID": 10
}
```

and

```json
10
```

As it can inspect the data at runtime.

Likewise, the Avro deserializer can inspect the schema of the data retrieved to determine if it 
is receiving an Avro `record` with a single integer field `ID`.

This approach has the benefit that users of KSQL don't need to know or worry about whether their
source data is wrapped or unwrapped. This is a big win! Unfortunately, it comes at a cost:
there are many areas where the choice between processing the data as an anonymous single field
or a wrapped named field becomes ambiguous. 

JSON runs into ambiguity when presented with a single ``MAP`` or ``STRUCT`` field. Both of 
which are serialized as JSON objects and without a schema it is hard to know if the received
JSON object is a wrapped field or just the field. 

AVRO is better, but still struggles with ``STRUCT`` fields.

The deserializers can go some way by seeing if values can be coerced to either schema, but this
adds overhead and there are still cases, especially in the presence of null values, where there
are edge cases where it is simply unclear which way the deserializer should go.

This ambiguity is the reason this alternative has been rejected in favour of more intuitive and
specific behaviour. 

## Have `WRAP_SINGLE_XXXX` be inherited in C\*AS queries from the source.

There were actually two proposed alternatives that saw `WRAP_SINGLE_XXXX` properties being inherited
in C*\AS queries: 

One where the property was treated as a _preference flag_, which controlled how a single-field schema
would be deserialized and serialized. The flag was always inherited, even if intermediate queries
had multi-field schemas.

In the other, the `WRAP_SINGLE_XXX` was not allowed in C* statements with multi-field statements
and implicitly set to `true` in multi-field C*\AS statements.

The former was rejected as the above approach is simpler. 

The latter was rejected as it meant a terminal query with a single-field schema would be wrapped,
even if the source C* statements explicitly stated unwrapped, the system default was unwrapped and
no intermediate queries explicitly changed the wrapping, _if_ any intermediate query's schema was
multi-field.

 


## Split `WRAP_SINGLE_XXX` into two properties: one for for deserialization and one for serialization.

Because the current `WRAP_SINGLE_XXXX` family of `WITH` clause properties controls both deserialization 
and serialization of single field schemas it is not possible to have a C* statement
where the value will be _deserialized unwrapped_, but downstream 
queries will _serialize_ single field schemas _wrapped_, or vice-versa.

An alternative would be to separate `WRAP_SINGLE_XXXX` (and the underlying system properties) into
properties specific to deserialization and serialization.

Naming is a challenge here. But for arguments sake lets go with:

* `WRAPPED_SINGLE_VALUES` to control deserialization, as in 'the single values are wrapped'
* `WRAP_SINGLE_VALUE` to control serialization, as in 'please wrap single values'

Though the names aren't so important.  Reworking the example from about you get something like:

```sql
-- Default config: ksql.persistence.deserialization.wrapped.single.value=false
-- Default config: ksql.persistence.serialization.wrap.single.value=false
 
-- creates a stream:
--  WRAPPED_SINGLE_VALUES: indicating that the source data has unwrapped values
--  WRAP_SINGLE_VALUE: indicates that downstream queries will wrap by default
CREATE STREAM UNWRAPPED_EXPLICIT_SOURCE (ID INT) WITH (WRAPPED_SINGLE_VALUES=false, WRAP_SINGLE_VALUE=true, ...);
  
-- creates a stream:
--   ksql.persistence.deserialization.wrapped.single.value indicating that the source data is not wrapped
--   ksql.persistence.serialization.wrap.single.values indicates that downstream queries will NOT wrap values by default
CREATE STREAM IMPLICIT_SOURCE (ID INT) WITH (...);
 
-- creates a stream:
--   with multiple fields:, so KSQL knows they'll be wrapped.
--   WRAP_SINGLE_VALUE: indicates downstream queries will NOT wrap values by default
CREATE STREAM MULTI_FIELD_SOURCE (ID INT, NAME STRING) WITH (WRAP_SINGLE_VALUE=false, ...);
 
-- will result in error as `WRAPPED_SINGLE_VALUES` can not be `false` for multi-field schema.
CREATE STREAM BAD_SOURCE (ID INT, NAME STRING) WITH (WRAPPED_SINGLE_VALUES=false, ...);
 
-- KSQL knows the source data is wrapped because the source is flagged as such
-- serialized value will be wrapped, due to inherited props
-- downstream queries will wrap values by default, due to inherited props
CREATE STREAM A AS SELECT ID FROM EXPLICIT_SOURCE;
 
-- KSQL knows the source data is wrapped because the source is flagged as such
-- serialized values will NOT be wrapped, due to with clause
-- downstream queries will NOT wrap values by default, due to with clause
CREATE STREAM B WITH(WRAP_SINGLE_VALUE=false) AS SELECT ID FROM EXPLICIT_SOURCE;
 
-- KSQL knows the source data is not wrapped because the source is flagged as such and has single field schema
-- serialized value will NOT be wrapped, due to inherited props
-- downstream queries will NOT wrap values by default, due to inherited props
CREATE STREAM C AS SELECT ID FROM IMPLICIT_SOURCE;
 
-- KSQL knows the source data is not wrapped because the source is flagged as such and has single field schema
-- serialized value will be wrapped, due to with clause
-- downstream queries will wrap values by default, due to with clause
CREATE STREAM D WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID FROM IMPLICIT_SOURCE;
 
-- KSQL knows the source data is wrapped as it has multiple fields
-- serialized value will be wrapped as it has multiple fields
-- downsteam queries will NOT wrap single values by default, due to inherited props
CREATE STREAM E AS SELECT ID, NAME FROM MULTI_FIELD_SOURCE;
 
-- KSQL knows the source data is wrapped as it has multiple fields
-- serialized value will be wrapped as it has multiple fields
-- downsteam queries will wrap single values by default, due to with clause
CREATE STREAM F WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID, NAME FROM MULTI_FIELD_SOURCE;
 
-- KSQL knows the source data is wrapped as it has multiple fields
-- serialized value will NOT be wrapped, due to inherited props
-- downsteam queries will NOT wrap single values by default, due to inherited props
CREATE STREAM G AS SELECT ID, FROM MULTI_FIELD_SOURCE;
 
-- KSQL knows the source data is wrapped as it has multiple fields
-- serialized value will be wrapped, due to with clause
-- downsteam queries will wrap single values by default, due to with clause
CREATE STREAM H WITH(WRAP_SINGLE_VALUE=true) AS SELECT ID FROM MULTI_FIELD_SOURCE;
```

Which is not the worse thing in the world, but it adds _another_ `WITH` clause property,
and will likely confuse some people.

It's also likely that people wanting specific control over wrapped / unwrapped single values will likely
want to stick with the same choice all the time.

Finally, it is still possible to control the downstream serialization explicitly within the downstream
`WITH` clause.

For these reasons this approach was rejected.
