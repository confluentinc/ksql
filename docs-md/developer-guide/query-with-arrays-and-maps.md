---
layout: page
title: Query With Arrays and Maps
tagline: SQL statements for complex data types in ksqlDB
description: Learn how to use complex types, like arrays and maps, in your ksqlDB queries.
---

ksqlDB enables using complex types, like arrays and maps, in your queries.
You use familiar syntax, like `myarray ARRAY<type>` and `myarray[0]` to
declare and access these types.

The following example shows how to create a ksqlDB table from an
{{ site.aktm }} topic that has array and map fields. Also, it shows how to
run queries to access the array and map data. It assumes a Kafka topic,
named `users`. To see this example in action, create the `users` topic
by following the procedure in
[Write Streaming Queries Against {{ site.aktm }} Using ksqlDB](../tutorials/basics-docker.md).

!!! important
      When you start the ksql-datagen process for the `users` topic, set the
      `quickstart` parameter to `users_`, to add the array and map fields to
      the `user` records. Be sure to append the `_` character. The array and
      map fields are named `interests` and `contactinfo` and have type
      ARRAY<STRING> and MAP<STRING,STRING>.

Create a Table With Array and Map Fields
----------------------------------------

Run the following query to create a ksqlDB table on the `users` topic. The
array and map fields are defined in the `interests ARRAY<STRING>` and
`contactinfo MAP<STRING,STRING>` declarations.

```sql
CREATE TABLE users
  (registertime BIGINT,
   userid VARCHAR,
   gender VARCHAR,
   regionid VARCHAR,
   interests ARRAY<STRING>,
   contactinfo MAP<STRING,STRING>)
  WITH (KAFKA_TOPIC = 'users',
        VALUE_FORMAT='JSON',
        KEY = 'userid');
```

Your output should resemble:

```
 Message
---------------
 Table created
---------------
```

The table is ready for you to run queries against it.

Query a Table That Has Array and Map Fields
-------------------------------------------

With the `users` table created, you can query the array field and the
map field. Run the following CREATE TABLE AS SELECT statement to create
a persistent query that has the first item in the user's `interests`
array and the user's city and zip code from the `contactinfo` map.

```sql
CREATE TABLE users_interest_and_contactinfo AS
  SELECT interests[0] AS first_interest,
         contactinfo['zipcode'] AS zipcode,
         contactinfo['city'] AS city,
         userid,
         gender,
         regionid
  FROM users
  EMIT CHANGES;
```

Your output should resemble:

```
 Message
---------------------------
 Table created and running
---------------------------
```

Run the following SELECT query to view the table:

```sql
SELECT userid, first_interest, city, zipcode
  FROM users_interest_and_contactinfo
  EMIT CHANGES;
```

Your output should resemble:

```
User_4 | Game | Palo Alto | 94301
User_9 | Game | San Jose | 95112
User_3 | News | San Mateo | 94403
User_6 | Game | Irvine | 92617
User_1 | Game | Irvine | 92617
User_7 | News | San Mateo | 94403
User_2 | News | Irvine | 92617
User_8 | Game | San Mateo | 94403
User_5 | Game | San Carlos | 94070
^CQuery terminated
```

Press Ctrl+C to terminate the query.

You can access array elements by using positive or negative index
values. For example, to get the user's last interest run the following
SELECT statement:

```sql
SELECT interests[-1] AS last_interest,
       userid,
       gender,
       regionid
FROM users_extended
EMIT CHANGES;
```

Your output should resemble:

```
Travel | User_9 | OTHER  | Region_6
Travel | User_2 | FEMALE | Region_5
Sport  | User_3 | FEMALE | Region_8
Movies | User_5 | OTHER  | Region_9
Movies | User_8 | MALE   | Region_1
Movies | User_1 | MALE   | Region_6
News   | User_4 | MALE   | Region_9
Movies | User_7 | OTHER  | Region_1
Sport  | User_6 | FEMALE | Region_5
^CQuery terminated
```

Press Ctrl+C to terminate the query.

Next Steps
----------

-   [Create a ksqlDB Table](create-a-table.md)
-   [Create a ksqlDB Stream](create-a-stream.md)
-   [Join Event Streams with ksqlDB](joins/join-streams-and-tables.md)

Page last revised on: {{ git_revision_date }}
