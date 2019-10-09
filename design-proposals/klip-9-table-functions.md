# KLIP 9 - Table functions

**Author**: Tim Fox (GitHub: purplefox)
**Release Target**: 5.4 
**Status**: In Discussion 
**Discussion**: https://github.com/confluentinc/ksql/issues/527

## Motivation and background

## Background

A _table valued function_ or _table generating function_ is a concept from the database world.
It's a function that returns a set of zero or more rows. Contrast this to a scalar function which
returns a single value. We will call them _table functions_ in the remainder of this document.

Table functions are typically used in FROM clauses with traditional databases. Some databases
such as PostgreSQL and Hive also allow table functions to appear in SELECT clauses.

An example of a table function from Apache Hive is `explode` - this takes a non scalar type such as
an ARRAY or MAP and explodes it to zero or more rows.

`select explode(array('A','B','C'));`

Would return three rows:

```
A
B
C
```

Table functions can be thought of as analagous to the `FlatMap` operation commonly found in 
functional programming or stream processing frameworks such as Kafka Streams.

Please see the following links for further background information:

* [Apache Hive Table Generating Functions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inTable-GeneratingFunctions(UDTF)
* [Microsoft SQL Server table functions](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/linq/how-to-use-table-valued-user-defined-functions)
* [Google BigQuery (see UNNEST)](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
* [PostgreSQL (see UNNEST)](http://www.postgresql.cn/docs/9.5/functions-array.html) 
* [Kafka Streams (see FlatMap)](https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html)

## Motivation

We propose to support table functions in SELECT clauses in KSQL.

There is [demand from KSQL users](https://github.com/confluentinc/ksql/issues/527) for a way to explode non scalar types such
as ARRAY or MAP from columns in KSQL streams or tables into multiple rows. This is the main
motivation for proposing this feature.

Table funtions would also be useful for other cases such as splitting a string into words, or any
case where you need to go from a single row to zero or more, for example, unaggregating data or
duplicating rows.

Here's an example of exploding an array from a column into multiple rows:

We have a stream containing a batch of sensor readings from a particular sensor. For further
stream processing we want to split them out into separate messages:

Given:

```
{sensor_id:12345 readings: [23, 56, 3, 76, 75]}
```

We want to get:

```
{sensor_id:12345 reading: 23}
{sensor_id:12345 reading: 56}
{sensor_id:12345 reading: 3}
{sensor_id:12345 reading: 76}
{sensor_id:12345 reading: 75}
```

This could be accomplished by the following CSAS:

```
CREATE STREAM exploded_stream AS
    SELECT sensor_id, EXPLODE(readings) AS reading FROM batched_readings;
```

Note that any scalar values in the select will be duplicated for each row in the output.


## What is in scope

We would like to implement

* Table functions on the SELECT clause only
* Multiple table functions on the SELECT clause
* A set of built-in table functions such as EXPLODE (etc)
* An ability for users to provide their own User Defined Table Functions (UDTFs) using annotations
in a similar way to how UDFs and UDAFs are currently supported.

## What is not in scope

* Table functions on the FROM clause. We could allow table functions to take the place of other streams
or tables in the FROM clause. We do not think there is much value in doing this as the main driver
for this KLIP is in supporting `explode` functionality which is more simply implemented on the
SELECT clause.
* Table functions that output more than one column. Some databases support the flattening of an array
 of N STRUCT types into N rows of M columns, where there is one column for each field of the struct.
 Instead we propose that flattening an array of N structs will result in N rows with a single column
 of type STRUCT. The fields of the struct can then be extracted if necessary using a further CSAS
 which pulls out the struct fields.

## Value/Return

This functionality will make it easier for users to make end-end streaming apps using KSQL
alone without having to have custom stages using Kafka Streams.

We believe its a common case for users to have array or map like data structures as values
in messages that they wish to explode. This new feature would allow them to do that in a pure KSQL
app thus allowing them to create richer KSQL applications.

## Public APIs

There will be no changes to the KSQL language or any existing public APIs or configuration files.

We will introduce new annotations to allow users to mark their UDTFs, in a very similar way to
UDAFs.

## Design

### General plumbing

We will need the following general plumbing to support table functions:

* In a similar way to how UDAFs are currently supported we will need to create a 
`KsqlTableFunction` which is a wrapper around the actual table function (whether built-in or
generated). We'll also need to augment the function registry to manage the addition and retrieval
of table functions.
* Augment the query analysis code to extract information relating to any table functions
in the query. We can do this either by extending the existing query analyzer or perhaps creating
a new `TableFunctionAnalyzer` in a similar way to how aggregate analysis uses its own 
`AggregateAnalyzer`.
* Augment the building of logical plan by building a new node representing a FlatMap stage of the
stream processing in the case there are table functions in the SELECT.
* Augment the execution plan logic by adding new nodes in the execution plan corresponding to any
extra FlatMap stage required by the query.
* Amend the columns used in the final projection to reflect any generated columns created by a 
FlatMap stage.
* Implement the actual FlatMap logic. For each row, any table functions on that row will be evaluated.
The number of rows returned from the FlatMap will be equal to the greatest of the number of rows
returned from any of the table functions for that row. Any non table function values for the row will
be duplicated for each row. For any table function that returned fewer than the greatest number of rows
the values for that columnn will be `null`. For performance reasons, the FlatMap logic should cache any
 table functions for the columns to avoid them being looked up each time.

### Implement set of built in table functions

We will implement the following built in table functions:

* `EXPLODE(ARRAY<V>)` -> Results in `N` rows (where `N` is number of elements in array) each with a column
of type `V` holding the value of the element
* `EXPLODE(MAP<K, V>)` -> Results in `N` rows (where `N` is number of elements in map) each with a column
holding a `STRUCT<key K, value V>` that contains the key and value.
* `INDEX_EXPLODE(ARRAY<V>)` -> Results in `N` rows (where `N` is number of elements in array) each with a
column of type `STRUCT<index INT, value V>` containing the index of the element from the original array
and the value from the array.

### Implement framework of user defined table functions

We will introduce annotations `@UdtfFactory` and `@UdtfDescription` in a similar way to UDAFs.

This will allow users to implement their own table functions which can be used in queries.

We will implement the loading of any UDTFs from the classpath at startup of the server.

## Test plan

New unit/module level tests will be written as appropriate.

A set of new QueryTranslationTest tests will be written to cover different ways of using table
functions in queries. These will cover all the built-in functions as well as testing examples of user defined table
functions.

## Documentation Updates

The following changes to the documentation will be made:

* Update syntax reference for the new built-in functions
* Add new section in the developer guide demonstrating how to use table functions to 'FlatMap'
streams
* Add a new KSQL example for table functions

# Compatibility Implications

There should be no compatibility implications as we are adding new functions only.

## Performance Implications

The new features will not affect performance of existing KSQL apps

## Security Implications

The new features will have the same security implications as allowing any user defined functions -
UDFs or UDAFs.

Allowing any kind of user defined extensions can introduce possible security issues such as:

* Malicious code extracting information and sending it to a third party
* Malicious code creating a denial of service by deliberately consuming memory, stalling execution,
or creating large amounts of results.
* Malicious code damaging the installation, e.g. by deleting files. 