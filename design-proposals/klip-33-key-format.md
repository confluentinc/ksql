# KLIP 33 - Key format

**Author**: @big-andy-coates | 
**Release Target**: 0.13 | 
**Status**: In Discussion | 
**Discussion**: https://github.com/confluentinc/ksql/pull/6017

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
 * Addition of new server configuration to provide defaults for key format.
 * Support for additional key column data types, where the key format supports it:
    * `DECIMAL`
    * `BOOLEAN`
 * Support of the following key formats:
    * `KAFKA`: the current key format.
    * `DELIMITED`: single key columns as a single string value.
    * `JSON` / `JSON_SR`: single key column as an anonymous value, i.e. not within a JSON object.
    * `AVRO`: single key column as an anonymous value, i.e. not within an Avro record.
    * `NULL`: special format indicating no data, or ignored data, e.g. a key-less stream.
  * Full support of these key formats for all supported SQL syntax.
  * Automatic repartitioning of streams and tables for joins where key formats do not match.
  * Support for reading & writing key schemas to & from the schema registry. 

## What is not in scope

 * Support for multiple key columns: this will come later.
 * Support for single key columns _wrapped_ in an envelope of some kind: this will come later.
 * Support for complex key column data types, i.e. array, struct and map 
 * Support for `PROTOBUF` keys, as this requires support for wrapped keys: this will come later.
 * Enhancing DataGen to support non-KAFKA keys.
 * Key schema evolution. (See [key schema evolution](#key-schema-evolution)) in the compatibility 
   section.

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

`KEY_FORMAT` will not be a required property _if_ `ksql.persistence.default.format.key` is set.

Providing `FORMAT` will set both the key and value formats. Providing `FORMAT` along with either 
`KEY_FORMAT` or `VALUE_FORMAT` will result in an error.

### Server configs

The following new configuration options will be added. These configurations can be set globally, 
within the application property file, or locally, via the `SET` command.

* `ksql.persistence.default.format.key`: the default key format.

## Design

The new `KEY_FORMAT` or `FORMAT` property will be supported where ever the current `VALUE_FORMAT` is
supported. Namely:

 * In `CRREATE STREAM` and `CREATE TABLE` statements.
 * In `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.
 
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

If a `CREATE TABLE` or `CREATE STREAM` statement does not include a `KEY_FORMAT` property, the 
key format is picked up from the `ksql.persistence.default.format.key` system configuration. If this
is not set, then an error is returned.  Note: The server config will have this set to `KAFKA` to 
maintain backwards compatibility with current system by default.

### Implementation

The system already serializes the key format of source, intermediate and sink topics as part of the
query plan, meaning it should be fairly easily to plug in new formats. 

Validation will be added to ensure only supported key formats are set, and that key column data types
are supported by key formats.

Most existing functionality should _just work_, as the key format only comes into play during 
(de)serialization, (obviously). The only area where additional work is expected are joins, single
key wrapping and key-less streams.

#### Joins

Joins require the binary key of both sides of the join to match and both sides to be delivered to 
the same ksqlDB node.  The former normally ensuring the latter, unless a custom partitioning 
strategy has been used.

The introduction of additional key formats means that while the deserialized key from both sides of 
a join may match, the serialized binary data may differ if the key serialization format is different.
To accommodate this, ksqlDB will automagically repartition one side of a join to match the key
format of the other.  

Choosing which side to reparation can not be driven by the size of the data, as in a traditional 
database system, as the size of the data is potentially infinite and the throughput of each side of
a join, unknown.

Hence the optimiser will base which side to repartition using an algorithm to minimise the number
of reparations.  For an N way join, the engine will determine which sources need to be repartitioned
anyway, as they are joining on something other than the key, and which don't. Changing the key 
format of a source that needs to be repartitioned anyway is 'free'. Changing the key format of a 
source that doesn't already need a repartition will have a fixed cost.  The formats with the lowest
repartitioning cost wins. 

Such repartitioning is possible and safe... ish, even for tables, because the logical key of the 
data will not have changed, only the serialization format. This ensures the ordering of updates to 
a specific key are maintained across the repartition. Of course, the repartitioning can, and 
probably will, introduce out-of-order data, as the records are shuffled across partitions. This 
phenomenon already exists for other implicit repartitions, and will be documented.

#### Single key wrapping   

To ensure query plans written after this work are forward compatible with future enhancements to 
support single key columns wrapped in JSON object, Avro records, etc, and ultimately multiple key 
columns, a new `UNWRAP_SINGLE_KEY` value will be added to `SerdeOption` and explicitly set on all
source, sink and internal topics. See [Future multi-column key work](#future-multi-column-key-work) 
below for more info / background.

#### Key-less streams

A new `NULL` format will be introduced to allow users to provide a `KEY_FORMAT` that informs ksqlDB
to ignore the key.  This format will be rejected as a `VALUE_FORMAT` for now, as ksqlDB does not yet
support value-less streams and tables. See [Schema Inference](#schema-inference) below for more 
info / background.

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
    * Addition of new server configuration to provide defaults for key format.
    * Support for additional key column data types, as JSON supports them:
        * `DECIMAL`
        * `BOOLEAN`
    * Full support of the key format for all supported SQL syntax.  
1. **NULL format**: Supported on keys only. Needed to support key-less streams once we have SR integration.
1. **Schema Registry support**: Adds support for reading and writing schemas to and from the schema
   registry.
1. **JSON_SR support** Adds support for the `JSON_SR` key format, inc. schema registry integration.
1. **Avro support** Adds support for the `AVRO` key format, inc. schema registry integration.
1. **Delimited support**: Adds support for the `DELIMITED` key format.
1. **Auto-repartitioning on key format mismatch**. Adds support for automatic repartitioning of 
   streams and tables for joins where key formats do not match.
1. **Blog post**: write a blog post about the new features. (Potentially more than once if 
  work span multiple releases).
   
LOE TBD once scope and design confirmed.

## Documentation Updates

New server config and new `CREATE` properties will be added to main docs site.

There are no incompatible changes within the proposal, so no demos and examples _must_ change.
However, it probably pays to update some to highlight the new features. We propose updating the 
Kafka micro site examples to leverage the new functionality, as these have automated testing.  
It may be worth changing the ksqlDB quickstart too. 

## Compatibility Implications

### Default to `KAFKA` key format

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

### Future multi-column key work

ksqlDB supports allowing the user to choose between serializing a single value column as 
an anonymous value, or within an envelope of some kind, via the `WRAP_SINGLE_VALUE` property. For 
example, the following expects the value to a `JSON` serialized number, _not_ a JSON object with 
a `foo` numeric field.   

```sql
CREATE STREAM INPUT (
   K STRING KEY, 
   foo INT
  ) WITH (
    WRAP_SINGLE_VALUE=false, 
    kafka_topic='input_topic', 
    value_format='JSON'
  );
```

The is also a system configuration `ksql.persistence.wrap.single.values` that can be used to provide
a default for wrapping / unwrapping single values. 

Where the user explicitly requests wrapping or unwrapping of single values, either via the `WITH` 
clause property or the system configuration, the query plan's `formats` has either the 
`WRAP_SINGLE_VALUES` or `UNWRAP_SINGLE_VALUES` `SerdeOption` set on the source and/or sink topics.

These options are used to ensure correct serialization and deserialization when the query is executed.
If neither option is set, the formats default wrapping is used, e.g. `KAFKA` defaults to unwrapped, 
where as `JSON` defaults to `wrapped`.

This KLIP adds the ability to serialized a single key column as an anonymous value. Future work will
extend this to support wrapped single columns and then multiple columns. This future work will need 
to maintain backwards compatibility and allow users to choose how single key values should be 
serialized. It will introduce a `WRAP_SINGLE_KEY` property and a `ksql.persistence.wrap.single.keys`
configuration.

To ensure query plans written by this KLIP are forwards compatible with this planned work, all query
plans will explicitly set the `UNWRAP_SINGLE_KEYS` `SerdeOption` on all source, internal and sink 
topics, ensuring the correct (de)serialization options once ksqlDB supports these later features. 

### Internal topics

NB: ksqlDB makes no claims of guaranteeing future versions will use the same formats for internal
    topics for new queries.  

Internal topics have their key format serialized as part of the query plan, i.e. all current plans
have `KAFKA` as the key format.  This means all existing plans are forward compatible with this
KLIP.

When generating query plans, internal topics inherit their key, (and value), format from their 
leftmost parent. This KLIP does not propose to change this, except where an automatic repartition 
is required to align key formats to enable a join, as already noted. New plans, generated after this
KLIP, may have key formats other than `KAFKA` for source, sink and internal topics. 

No changes around internal topics are needed to maintain compatibility.  

### Schema inference

The introduction of key formats that support schema inference, i.e. loading the key schema
from the schema registry, introduces an edge cases we must account for: key-less streams.

kqlDB already supports 'partial schemas', where the value format supports schema inference and the 
user explicitly provides the key definition: 

```sql
--- table created with values using schema inference.
CREATE TABLE FOO (
   ID BIGINT PRIMARY KEY
) WITH (
   KAFKA_TOPIC='foo',
   KEY_FORMAT='KAFKA',
   VALUE_FORMAT='AVRO'
);
```

If the key format also supports schema inference as well, then this would become:

```sql
--- table created with keys and values using schema inference.
CREATE TABLE FOO WITH (
   KAFKA_TOPIC='foo',
   KEY_FORMAT='KAFKA',
   VALUE_FORMAT='AVRO'
);
```

No problem so far. However, a user can currently define a key-less stream with:

```sql
-- key-less stream with explicitly provided columns: 
CREATE STREAM FOO (
   VAL STRING
) WITH (
    KAFKA_TOPIC='foo',
    VALUE_FORMAT='DELIMITED'
);

-- key-less stream with value columns using schema inference:
CREATE STREAM FOO WITH (
    KAFKA_TOPIC='foo',
    VALUE_FORMAT='AVRO'
);
```

But what happens once users can supply the key format? Key format currently defaults to `KAFKA`, but
it doesn't make sense to force users to set `KEY_FORMAT` to `KAFKA` if there is no key!

```sql
-- BAD!
CREATE STREAM FOO WITH (
    KAFKA_TOPIC='foo',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO'
);
```

The user may also have set a default key format, via the `ksql.persistence.default.format.key` 
system configuration, that supports schema inference. How then does a user declare a key-less 
stream as opposed to a stream where the key schema is loaded from the Schema Registry? 

```sql
SET 'ksql.persistence.default.format.key''='AVRO';' 

-- Is this key-less or loading its key from SR?
CREATE STREAM FOO WITH (
    KAFKA_TOPIC='foo',
    VALUE_FORMAT='AVRO'
);
```

We propose adding a `NULL` key format to allow users to explicitly set the key format when they
intend a stream to be key-less:

```sql
-- explicitly key-less stream:
CREATE STREAM FOO WITH (
    KAFKA_TOPIC='foo',
    KEY_FORMAT='NULL',
    VALUE_FORMAT='AVRO'
);
```

It will still be possible to define a key-less stream by not providing any key-column for key
formats don't support schema inference:

```sql
-- implicitly key-less stream:
CREATE STREAM FOO WITH (
    KAFKA_TOPIC='foo',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='AVRO'
);
```

### Key schema evolution

Key formats that support schema inference through integration with the Schema Registry prepend
the serialized key data with a magic byte and the id of the registered schema.

If the key schema evolves, the schema id will change. Hence the serialized bytes of the same
logical key will have changed, and meaning updates to the same logical key may now be spread across
multiple partitions. For this reason, evolution of the key's schema is not supported.

Is not supporting key schema evolution a big concern? We propose not. If the schema of the key has
changed, then in almost all cases the key itself has changed, e.g. a new column, or a change of 
column type. These, in themselves, will result in a different binary key. So the change of schema id
seems a secondary issue.

There are schema evolutions that would be binary compatible were it not for the schema id in the key, 
e.g. changing the logical type of a Avro value. It would be possible to add a custom partitioner that
ignored the magic byte and schema Id. However, we propose these are niche enough that supporting 
them has little ROI, at present. Hence key schema evolution is out of scope.   

## Security Implications

None.

[1]: https://docs.ksqldb.io/en/latest/developer-guide/serialization/#serialization-formats