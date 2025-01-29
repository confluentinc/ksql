# KLIP-5: Internal Schemas for KSQL Engine

**Author**: hjafarpour |
**Release Target**: 5.4 |
**Status**: In Discussion |
**Discussion**:

**tl;dr:** *Introduce the internal schemas for KSQL engine to decouple the internal identifier names from the
external field names so KSQL can handle any field name in the input data.*

## Motivation and background

Currently when users declare the schema for a stream or table, KSQL engine uses the provided schema
to perform query processing on the stream or table data. This means that the engine uses the same
names in the schema fields to refer to the schema fields in computations such as expression evaluation
 or filter evaluation. We generate Java code to evaluate such expressions which imposes certain
 restrictions on variable naming. However, requiring user schemas to comply with variable naming
 requirements in Java limits the KSQL ability to process any user data. Although in Avro and Delimited
 formats KSQL won’t have any issues since the schema field names already are compatible with the current
 KSQL naming requirements, in handling content with JSON format, KSQL would encounter limitations.
 In JSON format, field names don’t necessarily conform to KSQL naming requirements and a valid JSON
 message can have field names that cannot be accepted by KSQL. In addition to the above requirement,
 the field names in KSQL should also comply with the identifier naming requirements in SQL in order to
 have the statements parse correctly.

In this KLIP, we propose to decouple the internal schema, that KSQL uses for query processing, from
 the external schema that user content conforms to. The internal KSQL schema will be fully compliant
 with KSQL requirements and the engine can use it to process queries with no issues. On the other
 hand, external schema won’t be restricted with KSQL requirements and users can use any content with
 any naming convention in KSQL.

Users will be able to use quoted identifiers to declare and access fields that do not conform to
the KSQL identifier requirements. KSQL will generate an internal schema with a one to one mapping
from the external schema fields into the internal schema fields. All of the computation will use the
field names from the internal schema.

As an example, consider we want to declare s stream on a topic with values in JSON format. The following
is a sample record value in this topic:

```json
{"@ID": 0, "@NAME": "foo", "MESSAGE.AMOUNT": 0.0, "SELECT": "bar"}
```

As you can see, two of the field names start with “@” and the third one includes “.” in the field name.
The forth field name is "SELECT" which is a reserved word in KSQL.
 Currently we cannot declare a stream or table on this topic since its content does not conform to KQSL
 field naming requirements. However, we should be able to use quoted identifiers, which is supported by
 our parser, to declare a stream or table on this topic and write queries over the declared stream or
 table. Let’s assume we declare a stream on this topic, named foo. The following would be the DDL
  statement:

```sql
CREATE STREAM foo
  (“@ID” BIGINT, “@NAME” STRING, “MESSAGE.AMOUNT” DOUBLE, "SELECT" STRING)
WITH
  (KAFKA_TOPIC=’foo’, VALUE_FORMAT = ‘JSON’);
```

KSQL will add the above stream with the declared schema to its metastore. Now, let’s assume we want
to create a new stream from foo using the following CSAS statement:

```sql
CREATE STREAM bar AS
SELECT “@ID”, lowercase(“@NAME”) AS “@NAME”, “MESSAGE.AMOUNT” * 100 AS “AMOUNT.BY.100”, "SELECT"
FROM foo
WHERE “MESSAGE.AMOUNT” < 1;
```

KSQL will generate an internal schema for foo before processing the above query and rewrites the query
based on the internal schema. It then will use the rewritten query to process the records from the topic.

## Scope

* Support for Quoted Identifiers in DDL and DML statements. Only the identifiers for schema fields are in the scope for this KLIP and
identifiers for Stream/Table names are out of scope.
* Creation of internal schema for external schemas
* Query rewrite to use the internal schemas
* Query processing using the internal schemas

## Value/Return

This will eliminate the current requirement of having record field names in Kafka topics to conform
to KSQL internal requirements. With this KLIP, KSQL will be able to process topics with any naming convention.


## Public API

The only user facing part in this KLIP is the support for quoted identifier which will be the same
as the standard quoted identifiers in SQL. The declaration and use of the internal schemas are not
visible to users and are part of the KSQL engine internals.

## Design

The internal schema will only be used in executing DML statements. When the engine receives a DML
statement (CSAS, CTAS, INSERT INTO, SELECT), it builds an internal schema with valid field names for
the given source schema(s). This will be done by extending the current LogicalSchema class by adding
the internal schema to it. The internal schema field names can follow a predefined protocol so that
KSQL engine would build the same internal schema for a given external schema everytime. As an example,
 let’s consider we use the index of the field in the schema as a prefix to a constant string value to
 build the internal schema field names. Consider the previous example where our external schema has
 four fields as the following:

```sql
(“@ID” BIGINT, “@NAME” STRING, “MESSAGE.AMOUNT” DOUBLE, "SELECT" STRING)
```

Assuming the constant string prefix to build the internal schema fields is “COL_”, the internal
schema for the above schema will be as the following:

```sql
(COL_0 BIGINT, COL_1 STRING, COL_2 DOUBLE, COL_3 STRING)
```

Note that the following one to one mapping will exist between the fields from the external schema and the fields from the internal schema:

* “@ID”                            <==>   COL_0
* “@NAME”                     <==>   COL_1
* “MESSAGE.AMOUNT” <==>   COL_2
* "SELECT"         <==>   COL_3

In the case of nested fields, we can apply the same approach to the nested fields inside ARRAY, MAP
and STRUCT types.

Now that we have the internal schema built, the engine will rewrite the queries by replacing the
identifiers that refer to the fields in the external schema with their corresponding field name in
the internal schema.

As an example, consider the following query again:

```sql
CREATE STREAM bar AS
SELECT “@ID”, lowercase(foo.“@NAME”) AS “@NAME”, “MESSAGE.AMOUNT” * 100 AS “AMOUNT.BY.100”, "SELECT"
FROM foo
WHERE “MESSAGE.AMOUNT” < 1;
```

With the above mapping the engine will write this query as the following:

```sql

CREATE STREAM bar AS
SELECT FOO_COL_1 AS “@ID”, lowercase(FOO_COL_1) AS “@NAME”, FOO_COL_2 * 100 AS “AMOUNT.BY.100”, FOO_COL3
FROM foo
WHERE FOO_COL_2 < 1;
```

KSQL engine will be able to run the above query since all identifiers conform to the identifier
naming requirements in KSQL. Note that KSQL already appends the source name to the field name
automatically which will eliminate the ambiguity in case of having multiple sources (such as Join queries)
 with the same field names.

The sink schema for the above query will use the provided aliases for select expression as teh field names.
So the sink schema will be the following:

```sql
(“@ID” BIGINT, “@NAME” STRING, “AMOUNT.BY.100” DOUBLE, "SELECT" STRING)
```

The engine will use the generated internal schema to build the streams application. The deserializers
 from the source stream or table will use the external schema to fetch the fields and build the
 GenericRow object, but beyond this point the internal schema will be used to process the records
 for the queries.

Also note that the result schema will be generated from the external schemas which means if we select
any field from the external schema in the SELECT expressions and user does not specify an alias for
the result column, the engine will use the same name from the external schema for the corresponding
field in the result schema.

The engine should verify the result schema field names comply with the requirements of the sink format.
This verification should be done in query compile time. For instance, if the sink format is Avro, and the
field names in the result schema do not comply with Avro naming requirments, KSQL engine should not run
the query.

Note that all the KSQL error or log messages should still use the external schema names since user
does not know about the internal schemas.

## Test Plan

Tests for supporting quoted identifiers will be added. This includes tests for identifiers with periods, reserved words
such as "SELECT" or "ROWKEY", case sensitivity.

All of the existing tests for different stages of query processing will indeed test the correctness
 of the use of internal schemas too since the internal schemas will be used for such processing.

Tests that validate the intermediate schemas such as the ones in QTT should be updated since the
intermediate schemas will be based on the internal schemas.

## Documentation

Only documentation for the quoted identifier support will be added since that’s the only user facing feature.


## Compatibility Implications

The internal schemas would break compatibility for the stateful queries since the state store
currently stores values based on the external schema names.

## Performance Implications

No performance implication since we just change the identifier names and the processing logic does not change.

## Security Implications

No security implications.
