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
`CREATE` statement. Analogously, the key field will be taken from the same value as specified in the
`key` property. 

For the initial version, only primitive literal types will be supported (e.g. `INT`, `VARCHAR`, 
etc...) but syntax is easily extensible to support struct, map and list (i.e. by following syntax 
like Hive's `NAMED_STRCUT` construct). 

Future extensions: 
* Support some`AT INTERVAL` syntax to generalize the functionality to generate full stream data.
* Read from file to allow reproducible insertions

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

This functionality will be flagged under a configuration `ksql.create.source.allow.empty.topic`,
which will default to `true` (new behavior is the default).

## Design

### API Design

The above APIs make various trade-offs, outlined below:

* The proposal to overload `INSERT INTO` is to remain as compatible with SQL standard as possible.
The parser should be lookahead and determine whether or not `VALUES` is present in order to leverage
that data producing APIs. An alternative would be to use the word `PRODUCE`, which also makes it
clear that it uses low-level Kafka producers.
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

In interactive mode, this will be handled without distributing the statement to other servers. In
headless mode, the data will be produced on every machine that executes the SQL file if it contains
an `INSERT INTO ... VALUES` statement. It is important to support these operations even in headless
mode to build automated test pipelines.

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

## Test plan

This functionality is expected to work both in Interactive as well as Headless modes, and therefore
all tests described below should be done in both:

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
> explicitly sepcified values.
>
> Column values can be given in several ways:
> * Any column not explicitly given a value is set to its default (explicit or implicit) value.
> * If no columns are specified, a value for every column is expected in the same order as the
> schema
> * To produce a ``NULL`` value, specify only the key and contents for the key
>
> The values will serialize using the ``value_format`` specified in the original `CREATE` statment.
> ```

# Compatibility implications

* All previous commands that succeeded will still succeed after this KLIP
* `CREATE` commands that failed with "topic does not exist" may now fail with "must specify 
partitions and replicas to create source with empty topic."

# Performance implications

N/A
