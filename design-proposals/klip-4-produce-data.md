  # KLIP-4: Produce Data in KSQL

**Author**: agavra | 
**Release Target**: 5.3 | 
**Status**: In Discussion | 
**Discussion**: link

**tl;dr:** *Improve the interactivity of the development story by enabling CLI users to directly
produce data to Kafka topics through KSQL*

## Motivation and background

KSQL is a powerful tool for exploring Apache Kafka and stream processing paradigms. It allows a 
developer to easily create Streams and Tables from source topics, apply arbitrary transformations
and inspect resulting data in Kafka. For developers with relational database experience, the 
SQL-like language provides a familiar workflow.

After installing KSQL, developers are met with an immediate hurdle - they must have existing data 
in Kafka that match the data they expect to handle. In a relational database environment, developers
can use `INSERT INTO ... VALUES` to quickly construct a toy environment for testing. Generating this 
data KSQL, however, is cumbersome: 

* you require a separate tool (e.g. `kafkacat`, `ksql-datagen`)
* you must know the deployment topology (e.g. URIs for ZK/Broker)
* there is no synergy with KSQL (e.g. tools are not *schema aware* and can produce arbitrary data)

This KLIP attempts to integrate producing test data into the KSQL REPL.

## Scope

This KLIP is intended to improve the *development and testing* workflow as opposed to providing
*production-ready* functionality. The following tasks will be addressed:

* KSQL will be able to create and produce data to arbitrary topics
* KSQL will be able to validate data when producing to a topic that is registered with a schema
in KSQL (e.g. as a result of `CREATE STREAM`)

## Value

This proposal will improve the first 15-minute experience that developers have with KSQL and will
help seasoned KSQL developers test new functionality. Furthermore, this functionality can be
leveraged to create a powerful testing harness for KSQL applications that can run in a sandbox
without pre-existing data.

## Public APIS

#### INSERT INTO

```sql
INSERT INTO (stream_name|table_name) (column1, column2, ...)
  VALUES (value1, value2, ...)
```

The `INSERT INTO` statement can be used to insert new records into a kafka topic (agnostic to the
Stream/Table duality). If the columns are not present, it is assumed that the values are in the same
order as the schema and contain every field. The key for the insert statement is taken from the
column the matches the name of `key` in the `CREATE` statement properties for the corresponding
source. This functionality will need to be extended when we support structured keys.

The value for `stream_name`/`table_name` must be a valid Stream/Table registered with a KSQL schema.
The serialization format will be the same as the format specified in the `value_format` of the
`CREATE` statement. 

For the initial version, only primitive literal types will be supported (e.g. `INT`, `VARCHAR`, 
etc...) but syntax is easily extensible to support struct, map and list (i.e. by following syntax 
like Hive's `NAMED_STRCUT` construct). 

Future extensions: 
* Support some`AT INTERVAL` syntax to generalize the functionality to generate full stream data.
* Read from file to allow reproducible insertions

#### DELETE FROM

```sql 
DELETE FROM table_name WHERE condition;
```

The `DELETE` statement can be used to insert tombstones into kafka topics that back tables. If the
source is not back a table, the statement will fail. For this KLIP, we will only support conditions
of the form `ROWKEY = value`. While this may initially be confusing (as SQL standard allows deleting
on any arbitrary condition), we have precedent for this in stream-table joins (the join criteria
may only be on the key of the table).

#### CREATE STREAM/TABLE

```sql
CREATE STREAM stream_name ( { column_name data_type } [, ...] )
  WITH ( property_name = expression [, ...] );
```

```sql
CREATE TABLE table_name ( { column_name data_type } [, ...] )
  WITH ( property_name = expression [, ...] );
```

The `WITH` clause will now accept two additional property_names: `PARTITIONS`/`REPLICAS` If the 
topic *does not exist* and these values are specified, this command will create a topic with a the
set number of partitions and replicas before executing the command. If the topic exists and has
a different number of partitions/replicas, this command will fail.

This functionality will be flagged under a configuration `ksql.ddl.source.topic.create.allow`,
which will default to `true` (new behavior is the default).

## Design

### API Design

The above APIs make various trade-offs, outlined below:

* The proposal to overload `INSERT INTO` is to remain as compatible with SQL standard as possible.
The parser should be lookahead and determine whether or not `VALUES` is present in order to leverage
that data producing APIs. An alternative would be to use the word `PRODUCE`, which also makes it
clear that it uses low-level Kafka producers.
* The proposal to use `DELETE` encompasses various trade-offs:
  * We need a different mechanism to produce tombstones than `INSERT INTO` because there is no good
  way with the standard SQL to differentiate between an entirely `NULL` value (i.e. tombstone) and 
  `NULL` values for every column.
  * Introducing a new syntax (e.g. `INSERT TOMBSTONE`) will require users to understand another
  language construct and make KSQL depart further from SQL standard
  * This is a step closer to SQL standard, and we can always support more `DELETE` conditions in
  the future
* Allowing `CREATE TABLE/STREAM` to specify a kafka topic that does not yet exist makes it easy to
use the new APIs without any other system. There are three alternatives: 
  * Do not require `PARTITIONS`/`REPLICAS` to create the kafka topic. This is a slight change that
  will make it easier to experiment, but may open the door to accidentally creating kafka topics.
  * Introduce another `CREATE TOPIC` API which will create empty topics. I believe this will cause
  confusion, as it departs farther from SQL standard and puts `TOPIC` on the same level as the
  duality of Tables/Streams (which it is not, it is a physical manifestation of a stream/table).
  * Don't fail when creating streams/topics with non-existing topics, and create those topics if
  a call to `INSERT INTO ...` is called with a stream/table target that does not exist. This seems
  more "magical" and runs into issues when specifying partition/replica counts.

### Code Enhancements

KSQL in interactive mode is already configured to produce data to a command topic. This KLIP can
leverage that existing functionality, which will require piping through various configurations into
the `ServiceContext`. Beyond that, we must introduce a new command and Statement type to reflect the
new functionality. 

In interactive mode, this will be handled without distributing the statement to other servers. This
operation will not be available in headless mode for the first version of the feature.

### Schema Validation

`INSERT INTO ... VALUES` will only support inserting into values for existing KSQL Streams/Tables,
which already have a schema associated with them. Before producing a record to the underlying topic,
the values for the fields will be verified against the schema. If the columns contain `ROWKEY`, the
value will be verified against the key schema but will not populate the corresponding field in the 
value. Populating columns with `ROWTIME` will not be supported.

### Serialization Support

JSON, AVRO and DELIMITED value formats will all be supported, the latter of which will not accept
complex types. Since we know the schema of the inserts, we can initially load the values into a 
connect data type. We already have the proper serializers to convert from connect to each of the
supported serialization formats.

## Key Handling

The underlying data and the way it is represented in KSQL sometimes differ, and this `INSERT INTO`
functionality needs to bridge that gap. For example take the following KSQL statement:
```sql
CREATE TABLE bar (id VARCHAR, foo INT) WITH (key="id")
```

In this case, we expect kafka records that resemble 
```json
{"key": "bar", "value": {"id": "bar", "foo": 123}}
```

If this is not the case (e.g. the value of `"key"` and of `"value"->"id"` do not match), then the
KSQL behavior is not well defined. To all for all levels of flexibility, `INSERT INTO` should allow
for all cases that kafka supports. Therefore, `ROWKEY` will always be available in the list of
column names as the first column. The following examples would all be possible: 

```sql
INSERT INTO bar (ROWKEY, ID, FOO) VALUES ("k", null, 123);
INSERT INTO bar (ROWKEY, ID, FOO) VALUES ("k", "k", 456);
INSERT INTO bar (ROWKEY, ID, FOO) VALUES (null, "k", 789);
INSERT INTO bar (ROWKEY, ID, FOO) VALUES ("k", "x", 1011); 
INSERT INTO bar (ROWKEY, ID, FOO) VALUES ("k", null, null); 
```

If `ROWKEY` is _not_ specified but the key field is specified (as defined by the corresponding 
`CREATE TABLE` statement), then `ROWKEY` can be inferred, meaning the following statements are
equivalent:

```sql 
INSERT INTO bar (ID, FOO) VALUES ("k", 123)
INSERT INTO bar (ROWKEY, ID, FOO) VALUES ("k", "k", 123)
```

## Test plan

This functionality is expected to work both only in Interactive mode, and these tests must succeed:

* Test that values can be serialized into JSON/AVRO/DELIMITED
* Test that schemas are properly verified
* Test `NULL` and table tombstones
* Test `CREATE STREAM/TABLE` with topics that don't exist
* Test producing data in topics that already exist
* Test producing data to topics that are not associated with a KSQL source

## Documentation Updates

* The `CREATE TABLE` and `CREATE STREAM` syntax references will have an updated table properties
section that includes the following rows:
> ```rst
> +=========================+======================================================================================================+
> | KAFKA_TOPIC (required)  | The name of the Kafka topic that backs this source. The topic must either already exist in Kafka, or |
> |                         | PARTITIONS and REPLICAS must be specified to create the topic. Command will fail if the topic exists |
> |                         | with different partition/replica counts.                                                             |
> +-------------------------+------------------------------------------------------------------------------------------------------+
> | PARTITIONS              | The number of partitions in the backing topic. This property must be set if creating a SOURCE without|
> |                         | an existing topic.                                                                                   |
> +-------------------------+------------------------------------------------------------------------------------------------------+
> | REPLICAS                | The number of replicas in the backing topic. This property must be set if creating a SOURCE without  |
> |                         | an existing topic.                                                                                   |
> +-------------------------+------------------------------------------------------------------------------------------------------+
> ```
* A new section for `INSERT INTO ... VALUES` will be added to the `sytnax.rst`:
> ```rst
> **Synopsis**
> 
> .. code:: sql
>
>   INSERT INTO stream_name/table_name [( column_name [, ...] )]
>     VALUES (value [, ...])
> 
> **Description**
> 
> Produce a row into an existing stream or table and its underlying topic based on
> explicitly sepcified values. The first ``column_name`` of every schema is ``ROWKEY``, which
> defines the corresponding kafka key - it is expected to always match a corresponding column in 
> the value if the create statement specifies a key.
>
> Column values can be given in several ways:
> * Any column not explicitly given a value is set to ``null`` (KSQL does not support DEFAULT values)
> * If no columns are specified, a value for every column is expected in the same order as the
> schema
>
> The values will serialize using the ``value_format`` specified in the original `CREATE` statment.
> ```
* A new section for `DELETE` will be added to the `syntax.rst`:
> ```rst
> **Synopsis**
> 
> .. code:: sql
> 
>     DELETE FROM table_name WHERE condition;
>     
> **Description**
> 
> Delete a row from an existing table. The ``condition`` must be of form ``ROWKEY = value`` and the
> ``table_name`` must represent a Table (as opposed to Stream). This will issue a tombstone, producing
> a value to the underlying kafka topic with the specified key and a null value. Deleting a value that
> does not exist will not affect the table contents.
> ```


# Compatibility Implications

* All previous commands that succeeded will still succeed after this KLIP
* `CREATE` commands that failed with "topic does not exist" may now fail with "must specify 
partitions and replicas to create source with empty topic."
* A new configuration `ksql.insert.into.values.enabled` will config guard this whole feature suite

# Performance Implications

N/A

# Security Implications

* Authentication/Authorization: we will authenticate/authorize `INSERT INTO` statements under the
same systems that we use for transient queries
* Audit: For V1, we will not be auditing `INSERT INTO` statements, but we will provide a config to
disable this functionality completely
