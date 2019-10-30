.. _table-functions:


Using Table Functions With KSQL
###############################

A _table function_ is a function that returns a set of zero or more rows. Contrast this to a scalar
function which returns a single value.

Table functions are analagous to the `FlatMap` operation commonly found in
functional programming or stream processing frameworks such as [kstreams].

Table functions are used in the SELECT clause of a query. They cause the query to potentially
output more than one row for each input value.

The current implementation of table functions only allows a single column to be returned. This column
can be any valid KSQL type.

Here's an example of the ``EXPLODE`` built-in table function which takes an ARRAY and outputs one value
for each element of the array:

.. code:: sql

  {sensor_id:12345 readings: [23, 56, 3, 76, 75]}
  {sensor_id:54321 readings: [12, 65, 38]}


The following stream:

.. code:: sql

  CREATE STREAM exploded_stream AS
    SELECT sensor_id, EXPLODE(readings) AS reading FROM batched_readings;

Would emit:

.. code:: sql

  {sensor_id:12345 reading: 23}
  {sensor_id:12345 reading: 56}
  {sensor_id:12345 reading: 3}
  {sensor_id:12345 reading: 76}
  {sensor_id:12345 reading: 75}
  {sensor_id:54321 reading: 12}
  {sensor_id:54321 reading: 65}
  {sensor_id:54321 reading: 38}

When scalar values are mixed with table function returns in a SELECT clause, the scalar values
(in the previous example ``sensor_id``) are copied for each value returned from the table function.

You can also use multiple table functions in a SELECT clause. In this situation, the results of the
table functions are "zipped" together. The total number of rows returned is equal to the greatest
number of values returned from any of the table functions. If some of the functions return fewer
rows than others, the missing values are replaced with ``null``.

Here's an example that illustrates this.

With the following input data:

.. code:: sql

  {country:'UK', customer_names: ['john', 'paul', 'george', 'ringo'], customer_ages: [23, 56, 3]}
  {country:'USA', customer_names: ['chad', 'chip', 'brad'], customer_ages: [56, 34, 76, 84, 56]}

And the following stream:

.. code:: sql

  CREATE STREAM country_customers AS
    SELECT country, EXPLODE(customer_names) AS name, EXPLODE(customer_ages) AS age FROM country_batches;

Would give:

.. code:: sql

  {country: 'UK', name: 'john', age: 23}
  {country: 'UK', name: 'paul', age: 56}
  {country: 'UK', name: 'george', age: 3}
  {country: 'UK', name: 'ringo', age: null}
  {country: 'USA', name: 'chad', age: 56}
  {country: 'USA', name: 'chip', age: 34}
  {country: 'USA', name: 'brad', age: 76}
  {country: 'USA', name: null, age: 84}
  {country: 'USA', name: null, age: 56}


Built-in Table Functions
========================

KSQL comes with built-in table functions. For more information, see :ref:`ksql_table_functions`.