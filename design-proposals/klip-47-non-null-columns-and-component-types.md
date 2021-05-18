# KLIP 47 - NON NULL columns and component types

**Author**: Steven Zhang (@stevenpyzhang) | 
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: TBD-PR-LINK

**tl;dr:** _NOT NULL increases data integrity by putting constraints on column values in streams and tables._
           
## Motivation and background
https://github.com/confluentinc/ksql/issues/4436

The SQL standard allows a column to have a `NOT NULL` constraint. `NOT NULL` indicates that
`NULL` is not an accepted value for that column. This constraint prevents records with a `NULL` column value from being written into the table. 
When a user reads from the table, its guaranteed that every table record will have a value in the `NOT NULL` column.

Currently, all columns in KSQL are implicitly nullable. This KLIP would introduce `NOT NULL` syntax so users can apply it as a constraint to ksqlDB columns.
Columns that don't have `NOT NULL` constraint default to being nullable, which is the current behavior. When a `NOT NULL` constraint is applied to a column, 
records with null column values should not be written into constrained stream/table via `CSAS/CTAS/Insert Into` queries or `Insert Into Values` statements.
When a user reads from a ksqlDB stream/table with a `NOT NULL` column, they should not read any records that violate the `NOT NULL` constraint.

Being able to specify `NOT NULL` as part of a stream/table schema would also help improve the integration with Schema Registry.
All ksqlDB columns must have an internal SQL type associated with it. When ksqlDB converts its internal SQL types to a Connect Schema, the schema is always optional (nullable) because there's no 
existing mechanism/syntax to indicate the type is not nullable. This leads to some undesirable behavior when users use Schema Registry.
For example, if a map key is nullable, Schema Registry will interpret the map schema as a list of key/value map entries. Since ksqlDB map keys
are always nullable, ksqlDB will take schemas with maps and write them to Schema Registry as a list of key/value map entries, which is a confusing behavior for users.
Having map keys that are `NOT NULL` would allow ksqlDB maps to be represented properly in Schema Registry.
* https://github.com/confluentinc/ksql/issues/6049
* https://github.com/confluentinc/ksql/issues/2619

For the rest of the KLIP, I'll refer to any stream/table with a `NOT NULL` column as a `NOT NULL` stream/table.

### Data Ownership
Since ksqlDB relies on Kafka to store the data it reads/writes, the owner of the data in a topic isn't solely the ksqlDB service.
Producers outside ksqlDB are able to write into the topic. This means that the underlying topic for a `NOT NULL` stream/table can still
have null column records in it. There's no way around this as ksqlDB can't prevent outside producers from writing into the Kafka topics.

With that in mind, the `NOT NULL` constraint can be enforced within the ksqlDB service. When reading records from a source that has a `NOT NULL` 
column constraint, invalid records will be dropped, and an error sent to the processing log. When attempting to write an invalid
record to a sink, the record would also be dropped, and an error sent to the processing log. A `INSERT INTO VALUES` statement with
a null column record would error out. There would be clear documentation for users that `NOT NULL` does not prevent invalid records in the 
Kafka topic due to records from producers outside ksqlDB.

If there are concerns that applying a constraint over existing data (declaring a stream/table over an existing topic) would lead to records getting dropped, 
we could require a `DEFAULT` value be specified with `NOT NULL`. This is explained later in the KLIP and needs to be discussed further
 as it's not within scope of the original issue filed.

## What is in scope
* Allowing users to specify `NOT NULL` for columns
* Prevent reading records with a null column value from `NOT NULL` streams/tables
* Prevent writing records with a null column value to `NOT NULL` streams/tables
* Allow users to specify `NOT NULL` for array type, map key/value type, and struct field types
* (maybe) Allow users to specify a default value when applying `NOT NULL`

## Design
This design section is only focused on `NOT NULL` for now until we decide whether `DEFAULT` is required or not.

We can add a new `optional` variable to the `SqlType` class to indicate whether the type is optional or not. A `SqlType` variable is in each `Column` object so 
this would be a convenient way to store the `NOT NULL` constraint for each column. When doing code generation in `SqlToJavaVisitor`, we can then add null filtering code
in `visitUnqualifiedColumnReference` if the `optional` is present in the `SqlType` of the column. The set of `SchemaConverters` would need to be updated to account for non-optional `SqlType` 

For a stream/table created from a `CSAS/CTAS`, it would be possible for the user to enforce a `NOT NULL` constraint by casting the a `Select` value
with a `NOT NULL` type.

```sql
CREATE STREAM valid_data as select CAST(viewtime + 5 as BIGINT NOT NULL) ...
```
The `CastEvaluator` class would be updated accordingly to generate the code that prevents null records from being written into the sink

## Public APIS
`CREATE STREAM/TABLE` with `NOT NULL` column:
```sql
CREATE stream pageviews(viewtime BIGINT, userid VARCHAR NOT NULL, pageid VARCHAR NOT NULL) ...
```

Given data in the pageviews topic:
```sql
3,,
3,user,page
2,,page1
null,user,page
```

A `SELECT` query from the table would return only the two records that didn't violate the `NOT NULL` constraint:
```sql
ksql> select * from pageviews emit changes;
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|VIEWTIME       |USERID                                                          |PAGEID                                                          |
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|3              |user                                                            |page                                                            |
|null           |user                                                            |page1                                                           |
```

Declaring a column as `NOT NULL` in a CSAS/CTAS would look like:
```sql
CREATE stream pageviews_no_null as select CAST(viewtime AS BIGINT NOT NULL), userid, pageid from pageviews;
```
The resulting stream's columns would all be `NOT NULL`

```sql
ksql> select * from pageviews_no_null emit changes;
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|VIEWTIME       |USERID                                                          |PAGEID                                                          |
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|3              |user                                                            |page                                                            |
```

Attempting to insert invalid records would error out:
```sql
ksql> INSERT INTO pageviews VALUES (5, NULL, 'page23');
Failed to insert values into 'PAGEVIEWS': null violates the NOT NULL constraint for field "USERID"

ksql> INSERT INTO pageviews VALUES (NULL, 'user3', 'page23'); # this insert succeeds

ksql> INSERT INTO pageviews_no_null VALUES (NULL, 'user3', 'page23');
Failed to insert values into 'PAGEVIEWS': null violates the NOT NULL constraint for field "VIEWTIME"
```

`NOT NULL` for arrays, maps, struct
```sql
CREATE STREAM valid_data (arr ARRAY<BIGINT NOT NULL>, mapping MAP<VARCHAR NOT NULL, BIGINT NOT NULL>, valid_struct STRUCT<c DOUBLE NOT NULL, d BOOLEAN>) ...
```
Records that have column values such as
* `{arr: [3,4,null] ... }`
* `{mapping: {"key": null} ...}`
* `{mapping: {null: 5} ...}`
* `{valid_struct: {c: null, d: true} ...}`

Would be dropped when reading from the `valid_data` stream and trying to insert record with these column values would error out.

## NOT NULL DEFAULT
When `ALTER` is used to add a `NOT NULL` constraint in SQL to an existing table column, there can't be any `NULL` values 
in that column, otherwise there would be rows that violate the `NOT NULL` constraint.

In this case, the user needs to either `UPDATE` all existing `NULL` 
values in that column or specify a `DEFAULT` value for that column. All existing rows with `NULL` would have the default value added to that column, 
so that the NOT NULL constraint is not violated (This is only done if the `NOT NULL` constraint is used in conjunction with `DEFAULT`, if a 
column with no constraints has a `DEFAULT` applied to it, the existing rows with null columns won't be updated).

When `DEFAULT` is specified, instead of dropping the record, the default value would be applied to that record's column.
```sql
CREATE stream pageviews_default(viewtime BIGINT, userid VARCHAR NOT NULL DEFAULT("default_user"), pageid VARCHAR NOT NULL DEFAULT("default_page")) ...
```

A `SELECT` query would now return all the records
```sql
ksql> select * from pageviews_default emit changes;
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|VIEWTIME       |USERID                                                          |PAGEID                                                          |
+---------------+----------------------------------------------------------------+----------------------------------------------------------------+
|3              |default_user                                                    |default_page                                                    |
|3              |user                                                            |page                                                            |
|2              |default_user                                                    |page1                                                           |
|null           |user                                                            |page                                                            |
```

## Test plan
There will need to be tests for the following:
* QTT for queries with `NOT NULL` stream/table sources
* QTT for queries with `NOT NULL` stream/table sinks
* QTT preventing `INSERT VALUES` of null column values into `NOT NULL` streams/tables
* QTTs for `NOT NULL` array, map, and structs
* Existing QTTs are unaffected

## LOEs and Delivery Milestones
* Adding `NOT NULL` syntax (should error out and say `NOT NULL` is not implemented yet at this stage) - 2-3 days
* Update SQL types to indicate if a type is optional or not - 3 days
* Enforce `NOT NULL` when reading/writing from streams/tables - 1 week
* Expose the `NOT NULL` constraint in responses from the KSQL server and API Client 2-3 days
* `NOT NULL` for Array type (includes updating integration with Schema Registry) - 3 days
* `NOT NULL` for MAP key and value type (includes updating integration with Schema Registry) - 4 days
* `NOT NULL` in Struct fields (includes updating integration with Schema Registry) - 4 days
* (maybe) Implement `NOT NULL DEFAULT` - 1 week

## Documentation Updates
Documentation on the `NOT NULL` syntax and how applying it doesn't mean the Kafka topic won't have records 
with `NULL` values in the column.

## Compatibility Implications
None, the `NOT NULL` syntax would be a new modifier to a column so existing DDL statements would run without this modifier.

## Security Implications
None