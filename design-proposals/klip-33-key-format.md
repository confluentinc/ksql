# KLIP 33 - Key format

**Author**: @big-andy-coates | 
**Release Target**: 0.13 | 
**Status**: In Discussion | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** ksqlDB currently only supports keys compatible with the `KAFKA` format. This limits the
           data ksqlDB can work with. Extending the set of key formats ksqlDB supports immediately
           opens up the use of ksqlDB with previously incompatible datasets.
           
## Motivation and background

Data stored in Kafka has a key and a value. These can be serialized using different formats, but are
generally use a common format. ksqlDB supports multiple _value_ [data formats][1], but requires the 
key data format to be the `KAFKA`.  

This limitation is particularly problematic for tables. ksqlDB is unable to access changelog topics
in Kafka that have non-`KAFKA` formatted keys. As the key of the Kafka record is the `PRIMARY KEY` 
of the table, it is essential that the key can be read if the changelog is to be materialised into a 
table. When changelog topics have non-`KAFKA` key formats, the limitation precludes ksqlDB as a solution.

The limitation is less damaging for streams. However, it is still the case that the user looses the
ability to access the data in the Kafka record's key. If this data is not duplicated in the record's
value, which generally seems to be the case, then the data is not accessible at all. If the data is
required, then the limitation precludes ksqlDB as a solution.

As well as unsupported _input_ key formats, ksqlDB is equally precluded should a solution require the 
_output_ to have a non-`KAFKA` key format. ksqlDB is already often used as the glue between 
disparate systems, even though it is limited to changing the _values_ format and structure.
Supporting other key formats opens this up to also transforming the key into a different format. 

In some cases users are able to work around this limitation. This may involve changing upstream code,
or introducing pre-processing, or, in the case of Connect, using SMTs to convert the key format. All
such solutions tend to increase the complexity of the system, and generally hurt performance.

To open ksqlDB up to new problems spaces and to drive adoption, ksqlDB should support other key formats. 

## What is in scope

 * Addition of a new optional `KEY_FORMAT` property in the `WITH` clause, to set the key format.
 * Addition of a new optional `FORMAT` property in the `WITH` clause, to set both the key & value formats.
 * Addition of new server configuration to provide defaults for key and value formats.
 * Removal of requirement for `VALUE_FORMAT` property in the `WITH` clause of CT/CS statements, where
   the server configuration provides a default.
 * Support for additional key column data types, where the key format supports it:
    * `DECIMAL`
    * `BOOLEAN`
 * Support of the following key formats:
    * `DELIMITED`: single key columns as a single string value.
    * `JSON`: single key column as an anonymous value, i.e. not within a JSON object.
    * `AVRO`: single key column as an anonymous value, i.e. not within an Avro record.
  * Full support of these key formats for all supported SQL syntax.
  * Automatic repartitioning of streams and tables for joins where key formats do not match.
  * Support for reading & writing key schemas to & from the schema registry. 

## What is not in scope

 * Support for multiple key columns: this will come later.
 * Support for single key columns _wrapped_ in an envelope of some kind: this will come later.
 * Support for complex key column data types, i.e. array, struct and map 
 * Support for `PROTOBUF` keys, as this requires support for wrapped keys: this will come later.

## Value/Return

We know from customers and community members that there are a lot of people that have data with 
non-`KAFKA` formatted keys. This is the first step to unlocking that data and use-cases.

With support for `AVRO` and `JSON` key formats there are a lot of existing use-cases that suddenly
no longer require pre-processing, or tricky Connect SMTs configured and there are new use-cases,
which ksqlDB was previously unsuitable for, as documented in the motivation section, which it can 
now handle.

## Public APIS

### CREATE properties

The following new properties will be accepted in the `WITH` clause of `CREATE` statements for streams
and tables.

* `KEY_FORMAT`: sets the key format, works long the same lines as the existing `VALUE_FORMAT`.
* `FORMAT`: sets both the key and value format with a single property.

The existing `VALUE_FORMAT` property will no longer be required, if the server config provides a 
default. (See below).

### Server configs

The following new configuration options will be added. These configurations can be set globally, 
within the application property file, or locally, via the `SET` command.

* `ksql.persistence.default.format.key`: the default key format.
* `ksql.persistence.default.format.value`: the default value format.

## Design

### New CREATE properties

The new `KEY_FORMAT` or `FORMAT` property will be supported where ever the current `VALUE_FORMAT` is
supported. Namely:

 * In `CRREATE STREAM` and `CREATE TABLE` statements.
 * In `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
 
Providing `FORMAT` will set both the key and value formats. Providing `FORMAT` along with either 
`KEY_FORMAT` or `VALUE_FORMAT` will result in an error.

The key format will follow the same inheritance rules as the current value format. Namely: any 
derived stream will inherit the format of its leftmost source, unless the format is explicitly set
in the with clause.

For example:

```sql
-- Creates a table over a changelog topic with AVRO key and JSON value:
CREATE TABLE USERS (
    ID BIGINT PRIMARY KEY,
    NAME STRING
  ) WITH (
    KAFKA_TOPIC='USERS',
    KEY_FORMAT='AVRO',
    VALUE_FORMAT='JSON'
  );

-- Creates a stream over a topic with JSON key and value:
CREATE STREAM BIDS (
    ITEM_ID BIGINT KEY,
    USER_ID BIGINT,
    AMMOUNT INT
  ) WITH (
    KAFKA_TOPIC='USERS',
    FORMAT='JSON'
  );

-- Change the key format of a stream:
CREATE STREAM AVRP_BIDS 
  WITH WITH (
    KEY_FORMAT='AVRO'
  ) AS
    SELECT * FROM BIDS;

-- Creates an enriched stream. The key format is inherited from the leftmost source, i.e. JSON:
CREATE STREAM ENRICHED_BIDS AS 
  SELECT *
  FROM BIDS 
   JOIN USERS ON BIDS.USER_ID = USERS.ID;
```

For formats that support integration with the schema registry, the key schema will be read and 
registered with the Schema Registry as needed, following the same pattern as the value schema in 
the current product.

### New server config

In addition to new `CREATE` properties, the user will also be able to set default key and value 
formats using system configuration. 

`KEY_FORMAT` will not be a required property _if_ `ksql.persistence.default.format.key` is set. 
`VALUE_FORMAT` is currently a required property, but will no longer be required _if_ 
`ksql.persistence.default.format.value` is set.

Somewhat unrelated, but small, we propose also changing the `KAFKA_TOPIC` property from _required_ to 
_optional_ in `CREATE STREAM` and `CREATE TABLE` statements. If not supplied, it will default to the 
uppercase name of the stream or table.  This matches the behaviour of the property in 
`CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements, where it is already optional.

To additional configurations will be added to control topic names:

* `ksql.persistence.topic.name.case`: if `lower`, defaults ksql to using lowercase topics names
  where topic names are not explicitly provided. This includes matching a `CREATE TABLE` or 
  `CREATE STREAM` statement to an existing topic. Default: `upper`.
* `ksql.persistence.topic.name.case.insensitive`: if `true`, matching a `CREATE TABLE` or 
  `CREATE STREAM` statement to an existing topic will be case insensitive. Multiple matches will 
  result in an error. Default: `false`.

With these changes there will no longer be any required `CREATE` properties. This means the `WITH` 
clause for `CREATE` statements for streams and tables would be optional. We propose this makes the 
syntax more intuitive for those already familiar with SQL. This would make a `CREATE TABLE` 
statement ANSI standard.

```sql
SET 'ksql.persistence.default.format.key'='Avro';
SET 'ksql.persistence.default.format.value'='Avro';

CREATE TABLE USERS (
  ID BIGINT PRIMARY KEY,
  NAME STRING
);

CREATE STREAM BIDS (
  ITEM_ID BIGINT KEY,
  USER_ID BIGINT,
  AMMOUNT INT
);
```

### Implementation

The system already serializes the key format of source, intermediate and sink topics as part of the
query plan, meaning it should be fairly easily to plug in new formats. 

Validation will be added to ensure only supported key formats are set, and that key column data types
are supported by key formats.

Most existing functionality should _just work_, as the key format only comes into play during 
(de)serialization, (obviously). The only area where additional work is expected is joins.

Joins require the binary key of both sides of the join to match and both sides to be delivered to 
the same ksqlDB node.  The former normally ensuring the latter, unless a custom partitioning 
strategy has been used.

The introduction of additional key formats means that while the deserialized key from both sides of 
a join may match, the serialized binary data may differ if the key serialization format is different.
To accommodate this, ksqlDB will automagically repartition the right side of a join to match the key
format of the left side.  Such repartitioning is possible and safe, even for tables, because the 
logical key of the data will not have changed, only the serialization format. This ensures the 
ordering of updates to a specific key are maintained across the repartition.

## Test plan

Aside from the usual unit tests etc, the QTT suit of tests will be extended to cover the different
key formats. Tests will be added to cover the new syntax and configuration combinations and 
permutations. Existing tests covering aggregations, re-partitions and joins will be extended to 
include variants with different key formats. 

## LOEs and Delivery Milestones

The KLIP will be broken down into the following deliverables:

1. **Basic JSON support**: Support for the `JSON` key format, without:
    * schema registry integration
    * making KAFKA_TOPIC property optional
    * Automatic repartitioning of streams and tables for joins where key formats do not match: such
      joins will result in an error initially.
  
    Included in this milestone:
    
    * Addition of a new optional `KEY_FORMAT` property in the `WITH` clause, to set the key format.
    * Addition of a new optional `FORMAT` property in the `WITH` clause, to set both the key & value formats.
    * Addition of new server configuration to provide defaults for key and value formats.
    * Removal of requirement for `VALUE_FORMAT` property in the `WITH` clause of CT/CS statements, where
      the server configuration provides a default.
    * Support for additional key column data types, as JSON supports them:
        * `DECIMAL`
        * `BOOLEAN`
    * Full support of these key formats for all supported SQL syntax  
1. **Auto-repartitioning on key format mismatch**. Adds support for automatic repartitioning of 
   streams and tables for joins where key formats do not match.
1. **Schema Registry support**: Adds support for reading and writing schemas to and from the schema
   registry.
1. **JSON_SR support** Adds support for the `AVRO` key format, inc. schema registry integration.
1. **Avro support** Adds support for the `AVRO` key format, inc. schema registry integration.
1. **Delimited support**: Adds support for the `DELIMITED` key format.
1. **Optional KAFKA_TOPIC property**: makes the `KAFKA_TOPIC` property optional everywhere and add
   configuration to control case and case sensitivity of topic names.
1. **Blog post**: write a blog post about the new features. (Potentially more than once if 
  work span multiple releases).
   
LOE TBD once scope and design confirmed.

## Documentation Updates

New server config and new `CREATE` properties will be added to main docs site.

There are no incompatible changes within the proposal, so no demos and examples _must_ change.
However, it probably pays to update some to highlight the new features. We propose updating the 
Kafka micro site examples to leverage the new functionality, as these have automated testing.  
It may be worth changing the ksqlDB quickstart too - TBD, as this will require extending DataGen 
to support other key formats. Something we may want in scope anyway - or should be end-of-life 
DataGen in favour of the datagen connector?

## Compatibility Implications

As mentioned above, existing query plays already include key formats for all topics. So existing
queries will continue to work.

Without `ksql.persistence.default.format.key` set to `KAFKA` existing queries in the form:

```sql
CREATE TABLE USERS (
    ID BIGINT PRIMARY KEY,
    NAME STRING
  ) WITH (
    KAFKA_TOPIC='USERS',
    VALUE_FORMAT='JSON'
  );
```

...will start failing, as they do not specify the `KEY_FORMAT`. We therefore propose the server 
config shipped with ccloud and on-prem releases has `ksql.persistence.default.format.key` set to 
`KAFKA`.

Assuming the default key format is set, existing SQL will run unchanged. 

## Security Implications

None.

[1]: https://docs.ksqldb.io/en/latest/developer-guide/serialization/#serialization-formats