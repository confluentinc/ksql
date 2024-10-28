---
layout: page
title: ksqlDB Table Functions
tagline: Functions that return 0 or more rows
description: Learn how to use table function in a SELECT clause  
keywords: ksqldb, SQL, table, function, select
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/table-functions.html';
</script>

## Synopsis

A table function is a function that returns a set of zero or more rows.
Contrast this to a scalar function, which returns a single value.

Table functions are analogous to the `FlatMap` operation commonly found in
functional programming or stream processing frameworks such as
{{ site.kstreams }}.

!!! important
    Table functions are supported only on stream sources. 

Table functions are used in the SELECT clause of a query. They cause the query
to output potentially more than one row for each input value.

The current implementation of table functions only allows a single column
to be returned. This column can be any valid SQL type.

Here's an example of the `EXPLODE` built-in table function, which takes an
ARRAY and outputs one value for each element of the array:

```sql
  {sensor_id:12345 readings: [23, 56, 3, 76, 75]}
  {sensor_id:54321 readings: [12, 65, 38]}
```

The following stream:

```sql
  CREATE STREAM exploded_stream AS
    SELECT sensor_id, EXPLODE(readings) AS reading FROM batched_readings;
```

Would emit:

```sql
  {sensor_id:12345 reading: 23}
  {sensor_id:12345 reading: 56}
  {sensor_id:12345 reading: 3}
  {sensor_id:12345 reading: 76}
  {sensor_id:12345 reading: 75}
  {sensor_id:54321 reading: 12}
  {sensor_id:54321 reading: 65}
  {sensor_id:54321 reading: 38}
```

When scalar values are mixed with table function return values in a SELECT
clause, the scalar values, like `sensor_id` in the previous example, are
copied for each value returned from the table function.

You can also use multiple table functions in a SELECT clause. In this
situation, the results of the table functions are "zipped" together. The total
number of rows returned is equal to the greatest number of values returned from
any of the table functions. If some of the functions return fewer rows than
others, the missing values are replaced with ``null``.

Here's an example that illustrates using multiple table functions in a SELECT
clause.

With the following input data:

```sql
  {country:'UK', customer_names: ['john', 'paul', 'george', 'ringo'], customer_ages: [23, 56, 3]}
  {country:'USA', customer_names: ['chad', 'chip', 'brad'], customer_ages: [56, 34, 76, 84, 56]}
```

And the following stream:

```sql
  CREATE STREAM country_customers AS
    SELECT country, EXPLODE(customer_names) AS name, EXPLODE(customer_ages) AS age FROM country_batches;
```

Would give:

```sql
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

## Functions

### **`CUBE_EXPLODE`**

```sql title="Applies to: array<br>Since: 0.7.0"
CUBE_EXPLODE(array[col1, ..., colN])
```

For the specified array of columns, outputs all of their possible combinations.

The `CUBE_EXPLODE` function produces _2^d_ new rows, where _d_ is the number of columns
given as parameter.

Duplicate entries for columns with `NULL` value are skipped.

For example, given the following input records:

```sql
{"topic": "test_topic", "key": "0", "value": {"col1": 1, "col2": 2}}
{"topic": "test_topic", "key": "1", "value": {"col1": 1, "col2": null}}
```

And the following stream: 

```sql
CREATE STREAM TEST (col1 INT, col2 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT cube_explode(array[col1, col2]) VAL FROM TEST;
```

The `CUBE_EXPLODE` function produces the following output:

```sql
{"topic": "OUTPUT", "key": "0", "value": {"VAL": [null, null]}}
{"topic": "OUTPUT", "key": "0", "value": {"VAL": [null, 2]}}
{"topic": "OUTPUT", "key": "0", "value": {"VAL": [1, null]}}
{"topic": "OUTPUT", "key": "0", "value": {"VAL": [1, 2]}}
{"topic": "OUTPUT", "key": "1", "value": {"VAL": [null, null]}}
{"topic": "OUTPUT", "key": "1", "value": {"VAL": [1, null]}}
```

---


### **`EXPLODE`**

```sql title="Applies to: array<br>Since: 0.6.0"
EXPLODE(array)
```

Outputs one value for each of the elements in `array`.

The output values have the same type as the array elements.