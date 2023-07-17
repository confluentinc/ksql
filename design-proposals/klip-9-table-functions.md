# KLIP 9 - Table functions

**Author**: Tim Fox (GitHub: purplefox) |
**Release Target**: 5.4 |
**Status**: _Merged_ |
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

There is [demand from KSQL users](https://github.com/confluentinc/ksql/issues/527) for a way to explode
ARRAYs from columns in KSQL streams or tables into multiple rows. This is the main
motivation for proposing this feature.

Table functions would also be useful for other cases such as splitting a string into words, or any
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

We will also support multiple table functions in a SELECT clause. In this situation, the results of the
table functions are `zipped` together.

The total number of rows returned is equal to the greatest number of values returned from any of the
 table functions. If some of the functions return fewer rows than others, the missing values are
 replaced with `null`.
 
Here's an example of two table function results being zipped together:

With the following input data:

```
{country:'UK', customer_names: ['john', 'paul', 'george', 'ringo'], customer_ages: [23, 56, 3]}
{country:'USA', customer_names: ['chad', 'chip', 'brad'], customer_ages: [56, 34, 76, 84, 56]}
```
And the following stream:

```
CREATE STREAM country_customers AS
    SELECT country, EXPLODE(customer_names) AS name, EXPLODE(customer_ages) AS age FROM country_batches;
```
Would give:

```
{country: 'UK', name: 'john', age: 23}
{country: 'UK', name: 'paul', age: 56}
{country: 'UK', name: 'george', age: 3}
{country: 'UK', name: 'ringo', age: null}
{country: 'USA', name: 'chad', age: 56}
{country: 'USA', name: 'chip', age: 34}
{country: 'USA', name: 'brad', age: 76}
{country: 'USA', name: null, age: 84}
{country: 'USA', name: null, age: 56}
```
 
## What is in scope

We would like to implement

* Table functions on the SELECT clause only
* Multiple table functions on the SELECT clause
* Implementation of the built-in table function EXPLODE
* A set of built-in scalar functions including ENTRIES and SERIES (see design section)
* An ability for users to provide their own User Defined Table Functions (UDTFs) using annotations
in a similar way to how UDFs are currently supported using annotations.

## What is not in scope

* Table functions on the FROM clause. We could allow table functions to take the place of other streams
or tables in the FROM clause. We do not think there is much value in doing this as the main driver
for this KLIP is in supporting `explode` functionality which is more simply implemented on the
SELECT clause. Also, expressing `zip` semantics using joins on a FROM clause would require new a new join
type.
* Table functions that output more than one column: Some databases support the flattening of an array
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
UDFs.

## Design

### General plumbing

We will need the following general plumbing to support table functions:

* In a similar way to how UDFs are currently supported we will need to create a 
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
 
### UDTF API

Users will be able to implement their own table function by creating their classes containing
methods annotated with `@Udtf`.

A UDTF method can contain zero or more parameters, of any of the valid KSQL types mapped to Java.
The return value must be a `List<T>` where T is any valid KSQL type mapped to Java. 

```
@UdtfDescription(name = "SplitSpring", author = "bob", description = "Splits a string into words")
public class SplitString {

    @Udtf(description="Splits a string into words")
    public List<String> split(String input) {
        return Arrays.asList(String.split("\\s+"));
    }

}

```

UDTFs will support `@UdtfDescription` and `@UdtfSchemaProvider` in a similar way to UDFs.

### Implement set of built in table functions

We will implement the following built in table functions:

* `EXPLODE(ARRAY<V>)` -> Results in `N` rows (where `N` is number of elements in array) each with a column
of type `V` holding the value of the element


### Implement set of built-in scalar functions

We will implement the following set of built-in scalar functions that will be helpful when used
with the `EXPLODE` table function:

* `ENTRIES(map MAP<K, V>, sorted BOOLEAN) ARRAY<STRUCT<K, V>>` -> Convert a Map to an array of key value
pairs. If `sorted` is true then the entries are sorted by the natural ordering of `K` otherwise
the entries will be in an undefined order.
* `SEQUENCE(start INT, end INT) ARARAY<INT>` - Results in an array of `start - end + 1` values with an `INT` value from `start`
to `end` exclusive. We will also create a `BIGINT` version of this.

## Test plan

New unit/module level tests will be written as appropriate.

A set of new QueryTranslationTest tests will be written to cover different ways of using table
functions in queries. These will cover all the built-in functions as well as testing examples of user defined table
functions.

## Documentation Updates

The following changes to the documentation will be made:

### Syntax reference

The following section should be added to the syntax reference:

>+------------------------+---------------------------+------------+---------------------------------------------------------------------+
>| Function               | Example                   | Input Type | Description                                                         |
>+========================+===========================+============+=====================================================================+
>| EXPLODE                | ``EXPLODE(col1)``         | Array      | This function takes an Array and outputs one value for each of the  |
>|                        |                           |            | elements of the array. The output values are of the type of the     |                                     |
>|                        |                           |            | array elements.                                                     |
>+------------------------+---------------------------+------------+---------------------------------------------------------------------+
>| SERIES                 | ``SERIES(col1, col2)`     | INT, INT   | This function takes two ints - start and end and output a range of  |
>|                        |                           |            | ints from start to end (exclusive).                                 |
>+------------------------+---------------------------+------------+---------------------------------------------------------------------+
>| SORTED_PAIRS           | ``SORTED_PAIRS(col1)`     | MAP        | This function takes a map and outputs a list of structs - one       |
>|                        |                           |            | for each key, value pair in the map. The output is sorted by the    |
>|                        |                           |            | natural ordering of the key before returning.                       |
>+------------------------+---------------------------+------------+---------------------------------------------------------------------+
>| UNSORTED_PAIRS         | ``UNSORTED_PAIRS(col1)`   | MAP        | This function takes a map and outputs a list of structs - one       |
>|                        |                           |            | for each key, value pair in the map. The order of the pairs is      |
>|                        |                           |            | undefined.                                                          |
>+------------------------+---------------------------+------------+---------------------------------------------------------------------+

### Developer Guide

Add the following new section of the developer guide in `table-functions.rst`

> .. _aggregate-streaming-data-with-ksql:
>
>Using Table Functions With KSQL
>###############################
>
>A _table function_ is a function that returns a set of zero or more rows. Contrast this to a scalar
>function which returns a single value.
>
>Table functions can be thought of as analagous to the `FlatMap` operation commonly found in
>functional programming or stream processing frameworks such as Kafka Streams.
>
>Table functions are used in the SELECT clause of a query. They cause the query to potentially
>output more than one row for each input value.
>
>The current implementation of table functions only allows a single column to be returned. This column
>can be any valid KSQL type.
>
>Here's an example of the `EXPLODE` built-in table function which takes an ARRAY and outputs one value
>for each element of the array:
>
>.. code:: sql
>
>  {sensor_id:12345 readings: [23, 56, 3, 76, 75]}
>  {sensor_id:54321 readings: [12, 65, 38]}
>
>
>The following stream:
>
>.. code:: sql
>
>  CREATE STREAM exploded_stream AS
>    SELECT sensor_id, EXPLODE(readings) AS reading FROM batched_readings;
>
>Would emit:
>
>.. code:: sql
>
>  {sensor_id:12345 reading: 23}
>  {sensor_id:12345 reading: 56}
>  {sensor_id:12345 reading: 3}
>  {sensor_id:12345 reading: 76}
>  {sensor_id:12345 reading: 75}
>  {sensor_id:54321 reading: 12}
>  {sensor_id:54321 reading: 65}
>  {sensor_id:54321 reading: 38}
>
>When scalar values are mixed with table function returns in a SELECT clause, the scalar values
>(in the above example `sensor_id`) are copied for each value returned from the table function.
>
>You can also use multiple table functions in a SELECT clause. In this situation, the results of the
>table functions are `zipped` together. The total number of rows returned is equal to the greatest
>number of values returned from any of the table functions. If some of the functions return fewer
>rows than others, the missing values are replaced with `null`.
>
>Here's an example that illustrates this.
>
>With the following input data:
>
>.. code:: sql
>
>  {country:'UK', customer_names: ['john', 'paul', 'george', 'ringo'], customer_ages: [23, 56, 3]}
>  {country:'USA', customer_names: ['chad', 'chip', 'brad'], customer_ages: [56, 34, 76, 84, 56]}
>
>And the following stream:
>
>.. code:: sql
>
>  CREATE STREAM country_customers AS
>    SELECT country, EXPLODE(customer_names) AS name, EXPLODE(customer_ages) AS age FROM country_batches;
>
>Would give:
>
>.. code:: sql
>
>  {country: 'UK', name: 'john', age: 23}
>  {country: 'UK', name: 'paul', age: 56}
>  {country: 'UK', name: 'george', age: 3}
>  {country: 'UK', name: 'ringo', age: null}
>  {country: 'USA', name: 'chad', age: 56}
>  {country: 'USA', name: 'chip', age: 34}
>  {country: 'USA', name: 'brad', age: 76}
>  {country: 'USA', name: null, age: 84}
>  {country: 'USA', name: null, age: 56}
>
>
>Built-on Table Functions
>========================
>
>KSQL comes with a set of built-in table functions. Please see the syntax reference for more
>information.

### Example of Table Function

Add a new KSQL example for table functions in the
[KSQL examples](https://docs.confluent.io/current/ksql/docs/tutorials/examples.html):

### UDTFs

Update the `implement-a-udf.rst` documentation to cover UDTFs too.

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
