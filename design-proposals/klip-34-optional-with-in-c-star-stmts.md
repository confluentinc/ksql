# KLIP 34 - Optional `WITH` clause in `CREATE STREAM` and `CREATE TABLE` statements

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** `CREATE STREEAM` and `CREATE TABLE` statements currently require a `WITH` clause as the
           user must supply the topic name and value format. We propose allowing users to set a
           default value format and a topic naming strategy via config, making the `WITH` clause 
           optional. Thereby making the `CREATE` statement more Ansi SQL and hence more intuitive
           to new users. 
           
## Motivation and background

Both `CREATE STREAM` and `CREATE TABLE` statements must currently have a `WITH` clause as there are
two required properties:

* `KAFKA_TOPIC`: sets the name of the Kafka topic the data is, or will be, stored in.
* `VALUE_FORMAT`: the format of the data in the Kafka topic record's value.

For example:

```sql
CREATE TABLE FOO (
   ID BIGINT PRIMARY KEY,
   VAL STRING
 ) WITH (
   KAFKA_TOPIC='foo',
   VALUE_FORMAT='JSON'
);
```

Switch these properties to _optional_ would allow more natural SQL syntax:

```sql
CREATE TABLE FOO (
   ID BIGINT PRIMARY KEY,
   VAL STRING
);
```

This is more intuitive and familiar to new users with a SQL background.

## Why required `KAFKA_TOPIC`?

It's very common to see `CREATE` statements where the `KAFKA_TOPIC` name matches the name of the stream
or table being created, maybe with different case.

`KAFKA_TOPIC` is already optional for `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.

There seems no reason to _require_ users to supply the topic name where it matches the name of the 
created stream or table.

## Why required `VALUE_FORMAT`?

Many companies standardize on a single format, e.g. they're an 'Avro House' or a 'JSON house'. It seems
strange to require each and every `CREATE STREAM/TABLE` statement to have to spell out the value format.

`VALUE_FORMAT` is already optional for `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.

[KLIP-33](https://github.com/confluentinc/ksql/pull/6017) is introducing a `KEY_FORMAT` to mirror the
`VALUE_FORMAT`. This will not be a required value. Instead, a system configuration 
`ksql.persistence.default.format.key` can be set to provide the default key format where no explicit
`KEY_FORMAT` is supplied.

Like `KEY_FORMAT`, users should be able to choose a default value format, rather than having to specify
it in every statement. If they're a 'JSON house' they can just set the default value format to `JSON`
and never worry about it again.  Alternatively, users could set the default in their session properties.

## What is in scope

* Addition of new server configuration to provide a default for value format.
* Removal of requirement for `VALUE_FORMAT` property in the `WITH` clause of CT/CS statements, where
  the server configuration provides a default.
* Removal of requirement for `KAFKA_TOPIC` property in the `WITH` clause of CT/CS statements.

## What is not in scope

Anything and everything else =)

## Value/Return

Clearer and cleaner syntax, with less unnecessary typing. Lowers barrier of entry for new users who
are already familiar with Ansi SQL.

## Public APIS

### Server configs

The following new configuration options will be added. These configurations can be set globally, 
within the application property file, or locally, via the `SET` command.

* `ksql.persistence.default.format.value`: the default value format. Default: none.

* `ksql.persistence.topic.name.case`: if `lower`, defaults ksql to using lowercase topics names
  where topic names are not explicitly provided. This includes matching a `CREATE TABLE` or 
  `CREATE STREAM` statement to an existing topic, and includes the case of sink topics created
  by `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT`. Default: `upper`.

* `ksql.persistence.topic.name.case.insensitive`: if `true`, matching a `CREATE TABLE` or 
  `CREATE STREAM` statement to an existing source topic, and matching `CREATE TABLE AS SELECT` and
  `CREATE STREAM AS SELECT` to an existing sink topic, will be case insensitive. Multiple matches 
  will result in an error. Default: `false`.
  
Optionally:

* `ksql.persistence.topic.partitions`: if set, the number of partitions to use when creating a topic.
  If the user issues a `CREATE TABLE` or `CREATE STREAM` and the topic does not already exist, then
  ksqlDB will create the topic with this number of partitions.   
  
  This introduces the potential for confusion, should the user mistype the source or topic name and
  ksqlDB creates a new topic, rather than using an existing topic. However, as users would need to
  opt-in by setting this config, we think this is acceptable.


### CREATE properties

The following properties will not longer be required in the `WITH` clause of `CREATE TABLE` and 
`CREATE STREAM` statements, though the user may still choose to supply them.
 
| `WITH` clause property |  Configuration required for property to be optional |
|------------------------|-----------------------------------------------------|
| `KAFKA_TOPIC`          | N/A                                                 |
| `VALUE_FORMAT`         | `ksql.persistence.default.format.value`             |

## Design

### Optional `KAFKA_TOPIC`

We propose changing the `KAFKA_TOPIC` property from _required_ to _optional_ in `CREATE STREAM` and 
`CREATE TABLE` statements. If not supplied, it will default to the uppercase name of the stream or 
table. This matches the behaviour of the property in `CREATE STREAM AS SELECT` and 
`CREATE TABLE AS SELECT` statements, where it is already optional.

Users can optionally set the `ksql.persistence.topic.name.case` system configuration to switch to 
lowercase names for source and sink topics.

Setting the `ksql.persistence.topic.name.case.insensitive` system configuration to `true` will mean 
`CREATE TABLE`, `CREATE SOURCE`, `CREATE TABLE AS SELECT` and `CREATE STREAM AS SELECT` statements 
will match existing topic that matches the name of the source, regardless of case.  Where there are 
multiple candidate matches, an error will be returned that includes all candidates.

### Optional `VALUE_FORMAT`

If the `ksql.persistence.default.format.value` system configuration is set, then the `VALUE_FORMAT`
in the `WITH` clause is optional. Providing it will override the default.

```sql
SET 'ksql.persistence.default.format.value'='Json';

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

#### Schema inference

For formats that support schema inference this would make the syntax very succinct:

```sql
SET 'ksql.persistence.default.format.value'='Avro';

-- create a table:
CREATE TABLE USERS (
  ID BIGINT PRIMARY KEY
)

-- create a keyed stream:
CREATE STREAM BIDS (
  ITEM_ID BIGINT KEY,
);

-- create a key-less stream:
CREATE STREAM BODS (
  ITEM_ID BIGINT KEY,
);
```

## Test plan

Some of the existing QTT tests will be updated to use the new `ksql.persistence.default.format.value`
system configuration and not provide the `VALUE_FORMAT`.  Such tests would fail if the generated 
query plan differed from different versions.

Additional QTT tests will be added to test error message when `ksql.persistence.default.format.value`
is not set and `VALUE_FORMAT` is not provided.

Additional QTT tests will be added to cover the different combinations and permutations of topic 
naming configurations and topic name case, including negative tests where there are multiple matches.

## LOEs and Delivery Milestones

While this is probably small enough for a single chunk of work, there are clearly two parts:

1. Make `VALUE_FORMAT` optional and add `ksql.persistence.default.format.value`.
1. Make `KAFKA_TOPIC` optional and add associated configuration.

## Documentation Updates

Update of `CREATE TABLE` and `CREATE STREAM` docs to reflect that `VALUE_FORMAT` and `KAFKA_TOPIC`
are no longer _required_.

Update some of the Kafka tutorial microsite examples, and the examples in the ksqlDB microsite, to 
use the new syntax and explicit `SET` commands, leaving some as they are so that both ways are covered. 

## Compatibility Implications

None.

## Security Implications

None.
