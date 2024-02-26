# KLIP-29: Explicit Keys

**Author**: @big-andy-coates | 
**Release Target**: 0.10.0; 6.0.0 | 
**Status**: _Merged_ | 
**Discussion**: [Github PR](https://github.com/confluentinc/ksql/pull/5530)

**tl;dr:** Up until now ksqlDB has added an implicit `ROWKEY STRING (PRIMARY) KEY` to a `CREATE TABLE`
or `CREATE STREAM` statement that does not explicitly define a key column. This KLIP proposes
removing this implicit column creation and instead requiring tables to define their PRIMARY KEY, and
changing `CREATE STREAM` to create a stream without a KEY column, should none be defined.

## Motivation and background

Implicitly adding columns is confusing for users. They are left wondering where did this column come from? 
For example:

```
ksal> CREATE STREAM S (ID INT, NAME STRING) WITH (...);
Stream S Created.
ksal> CREATE STREAM S2 AS SELECT ID, NAME FROM S;
Key missing from projection. See https://cnfl.io/2LV7ouS.
The query used to build `S2` must include the key column ROWKEY in its projection.
```

Understandably, the user may be left wondering where this `ROWKEY` column came from and why they need to
add it to the projection.

Now that ksqlDB supports more than just `STRING` key columns, it no longer makes sense to add an
implicit `STRING` key column by default.  Better to let the user define _if_ there is a key column, and if
so, what it's name and type are.

There is no semantic need for a STREAM to have data in the key, and hence there should be no need for 
there to be a KEY column defined.  This can also be useful when the data in the key is not in a format 
ksqlDB can work with, e.g. an Avro, JSON or Protobuf key, or any other format.  It seems unwieldy and 
confusing to add an implicit `STRING` key column if the actual key is empty or is not something ksqlDB 
can read.

Unlike STREAMs, TABLEs require a PRIMARY KEY column. Therefore, we should require the user to provide one.  

## What is in scope

* `CREATE TABLE` statements will require a `PRIMARY KEY` column to be defined.
* `CREATE STREAM` statements without a `KEY` column defined will define a stream without a key column. 

## What is not in scope

* Everything else.

## Value/Return

Removal of implicitly added columns makes statements more declarative. They make it possible to describe 
a stream with no data, or incompatible data, in the key. They make users _think_ about the name and type
of the PRIMARY KEY column of their tables.

Combined, these changes make the language easier to use and richer.

## Public APIS

### CREATE TABLE changes

A `CREATE TABLE` statement that does not define a `PRIMARY KEY` column will now result in an error:

```
ksql> CREATE TABLE INPUT (A INT, B STRING) WITH (...);
Tables require a PRIMARY KEY. Please define the PRIMARY KEY.
```

Statements with a `PRIMARY KEY` column will continue to work as before.

Where schema inference is used, the error message will include an example of how to define a partial
schema to add the primary key:

```sql
ksql> CREATE TABLE INPUT WITH (value_foramt='Avro', ...);
Tables require a PRIMARY KEY. Please define the PRIMARY KEY.
You can define just the primary key and still load the value columns from the Schema registry, for example:
  CREATE TABLE INPUT (ID INT PRIMARY KEY) WITH (value_foramt='Avro', ...);
```

### CREATE STREAM changes

A `CREATE STREAM` statement that does not define a `KEY` column will now result in a stream with no
data being read from the Kafka record's key.

```sql
CREATE STREAM INPUT (A INT, B STRING) WITH (...);
-- Results in INPUT with schema: A INT, B STRING, i.e. no key column.
```

Statements with a `KEY` column will continue to work as before.

## Design

This is mainly a syntax only change, as detailed about. 

Streams without a key column will work just like any other stream. However, `GROUP BY`, `PARTITION BY`
and `JOIN`s will _always_ result in a repartition, as the grouping, partitioning or joining expression
can never be the key column.

Internally, ksqlDB and the Kafka Streams library it leverages, heavily leverages a key-value model.
Where a stream is created without a key column, internally ksqlDB will treat the key as a `Void` type,
and the key will always deserialize to `null`, regardless of the binary payload in the Kafka record's
key.

Likewise, when a row from a stream without a key is persisted to Kafka, the key will be serialized as
`null`. 

## Test plan

Ensure coverage of key-less streams in QTT tests, especially for `GROUP BY`, `PARTITION BY`, and `JOIN`s.

## LOEs and Delivery Milestones

Single deliverable, with low loe (prototype already working).

## Documentation Updates

Suitable doc updates for the `CREATE TABLE` and `CREATE STREAM` statements will be done as part of the KLIP.

Plus updates to the rests of the ksqlDB docs, Kafka tutorials microsite and the Examples repo will be done
in tandem with other syntax changes.

Release notes to call out this change in behaviour.

## Compatibility Implications

CREATE statements submitted on previous versions of ksqlDB will continue to work as expected.

Users submitting previously written statements may see `CREATE TABLE` statements that previously ran,
now fail, and see `CREATE STREAM` statements create streams without a `ROWKEY STRING KEY` column. 

Users receiving an error when their `CREATE TABLE` statements fail will need to update their statements
to include a suitable `PRIMARY KEY` column.  Where the statement already contains the column set, the 
addition of the `PRIMARY KEY` column should be simple.  However, users may be more confused when the 
statement is making use of schema inference, i.e. loading the value columns from the Schema Registry,
for example:

```sql
-- existing create statement that loads the value columns from the Schema Registry: 
CREATE TABLE OUTPUT WITH (value_format='Avro', kafka_topic='input');
```

As ksqlDB does not _yet_ support loading the key schema from the Schema Registry the user must now
supply the `PRIMARY KEY` in a _partial schema_. (_Partial schema_ support was added in v0.9):

```sql
-- updated create statement that loads the value columns from the Schema Registry: 
CREATE TABLE OUTPUT (ID INT PRIMARY KEY) WITH (value_format='Avro', kafka_topic='input');
```

The error message shown to users when the primary key is missing will include information about
partial schemas if the statement is using schema inference.

The change to `CREATE STREAM` is more subtle, as nothing will fail. However, users can either elect to
stick with the key-less stream, or add a suitable `KEY` column, depending on which best fits their
use-case.  


## Security Implications

None.
