# KLIP-45: Materializing Tables for Query

**Author**: Chittaranjan Prasad (@cprasad1) | 
**Release Target**: 0.17 | 
**Status**: Proposal | 
**Discussion**: link

**tl;dr:** KSQL users who wish to use pull queries have trouble even getting to the starting line: 
creating a table that they can query. This proposal seeks to lower the barrier to entry in a low-effort, low-risk way
by adding an extra option of `MATERIALIZE = force|default` in the `WITH` clause to `CTAS` statements.

## Motivation and background

Suppose you have a topic and you simply want to query it as a table. We see people repeatedly attempting it this way:
```roomsql
CREATE TABLE users (
     id BIGINT PRIMARY KEY,
     usertimestamp BIGINT,
     gender VARCHAR,
     region_id VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON'
   );
```
Sadly, you can’t issue pull queries against users because it is only metadata. The system will tell you:
```shell script
Table 'users' is not materialized. KSQL currently only supports static queries on materialized aggregate tables. i.e. those created by a 'CREATE TABLE AS SELECT <fields>, <aggregate_functions> FROM <sources> GROUP BY <key>' style statement.
```
The current workaround is something like this:
```roomsql
CREATE STREAM userstream (
     id BIGINT PRIMARY KEY,
     usertimestamp BIGINT,
     gender VARCHAR,
     region_id VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON'
   );

CREATE TABLE users AS
  SELECT id,
         LATEST_BY_OFFSET(usertimestamp) AS usertimestamp,
         LATEST_BY_OFFSET(gender) AS gender,
         LATEST_BY_OFFSET(region_id) AS region_id
  FROM userstream
  GROUP BY id
  EMIT CHANGES;
```
This has a number of problems:

1) It’s magic. There’s no way you’d think to do this unless you search the internet for “how to make a KSQL table queryable”.

2) It’s incredibly roundabout if you’re holding a table and just want to query it, as with every other database ever.

3) It doesn’t work. The specific mechanism here (a streaming aggregation) deliberately does not implement table semantics. Tables created this way will grow monotonically and incorrectly contain records that were deleted from the view upstream.

4) `LATEST_BY_OFFSET` (and all other aggregation functions) don’t work with complex types. So, it’s practically impossible to issue pull queries on anything but primitive data.

The same problem afflicts `CREATE TABLE AS SELECT`, meaning not only can’t you query tables created from topics, but you also can’t query tables derived from other tables.
## What is in scope

## What is not in scope

## Value/Return

## Public APIS

## Design

We will add `MATERIALIZE = force|default`  to `CTAS` statements. Here is a concrete example:
```roomsql
CREATE TABLE users (
     id BIGINT PRIMARY KEY,
     usertimestamp BIGINT,
     gender VARCHAR,
     region_id VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'my-users-topic', 
     VALUE_FORMAT = 'JSON'
   );
   
CREATE TABLE queriable_users
     WITH (MATERIALIZE = force)
     AS SELECT * from users;
```
This solves the basic problems with a small usability compromise:

1) Users still have to create two objects (two tables instead of a stream and a table) to get queriable table. I.e., it’s still kind of magical, but definitely more sensible. To mitigate the magic, we will improve the error messages to tell users exactly what to do if they try to query a base table or a non-materialized derived table.

2) It’s still moderately roundabout, but hopefully not too bad.

3) There are no longer any semantic issues of defining a stream->table transformation.

4) We side-step the problem with complex types in UDAFs
## Test plan

## LOEs and Delivery Milestones

## Documentation Updates

## Compatibility Implications


## Security Implications
