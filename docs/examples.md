# Examples

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start-guide) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | Examples | [FAQ](/docs/faq.md#faq)  | [Roadmap](/docs/roadmap.md#roadmap) | [Demo](/docs/demo.md#demo) |
|---|----|-----|----|----|----|----|----|

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

Here are some example queries to illustrate the look and feel of the KSQL syntax.
For the following examples we use 'pageviews' stream and 'users' table similar to the quickstart.
 The first step is to create stream and table in KSQL.

```sql
 CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');
```

The above statement creates a stream with three columns on the kafka topic that is named
'pageviews'. We should also tell KSQL in what format the values are stored in the topic. In this
example, the values format is 'DELIMITED'. The above statement does not make any assumption about
 the message key. However, if the value of message key is the same as one of the columns defined
 in the stream, we can provide such information in the WITH clasue. For instance, if the kafka
 message key has the same value as the 'pageid' column we can write the CREATE STREAM statement
 as follows:

 ```sql
  CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED', key='pageid');
 ```

If we want to use the value of one of the columns as the message timestamp, we can provide
such information to KSQL in the WITH clause. The message timestamp is used in window based
operations in KSQL and the feature provides event time processing in KSQL. For instance, if we want
 to use the value of 'viewtime' columns as the message timestamp we can rewrite the above statement as follows:

  ```sql
   CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH
   (kafka_topic='pageviews', value_format='DELIMITED', key='pageid', timestamp='viewtime');
  ```

To create a table we use CREATE TABLE statament. Here is the statement to create a user table
with four columns:

```sql
 CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid
 varchar, interests array<varchar>, contactinfo map<varchar, varchar>) WITH (kafka_topic='users',
 value_format='JSON');

```

As you can see the above table has a column with array type and another column with map type.
Currently, KSQL supports the following primitive data types: boolean, integer, bigint, double and
varchar(string). KSQL also supports array type with primitive elements and map type with varchar
(string) key and primitive type values.

Note that the above statements require the kafka topic that you define stream or table already
exists in your kafka cluster.

Now that we have the 'pageviews_original' stream and 'users_original' table, lets see some
example queries that you can write in KSQL. We focus on two types of KSQL statements, CREATE
STREAM AS SELECT and CREATE TABLE AS SELECT. For these statements KSQ
prsists the results of the query in a new stream or table which is backed by a Kafka topic.
Consider the following examples.

### Stream transformation
Let's say we want to create a new stream by transforming 'pageviews_original' in the following way:
- We want to add a new column that shows the timestamp in human readable string format.
- We want to have 'userid' column to be the key for the resulted stream.
- We want the kafka topic for the result stream have 5 partitions.
- We want to have 'viewtime' column value as the message timestamp in kafka topic.
- We want to have the results in JSON format.

The following statement will generate a new stream, 'pageview_transformed' with the above
properties:

```sql
 CREATE STREAM pageview_transformed WITH (partitions=5, timestamp='viewtime',
 value_format='JSON') AS SELECT viewtime,
 userid, pageid , TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HH:mm:ss.SSS') AS timestring FROM
 pageviews_original PARTITION BY userid;

```







### Filter an inbound stream of page views to only show errors

```sql
SELECT STREAM request, ip, status 
 WHERE status >= 400
```

### Create a new stream that contains the pageviews from female users only

```sql
CREATE STREAM pageviews_by_female_users AS
  SELECT users.userid AS userid, pageid, regionid, gender FROM pageviews
  LEFT JOIN users ON pageview.userid = users.userid
  WHERE gender = 'FEMALE';
```

### Continuously compute the number of pageviews for each page with 5-second tumbling windows

```sql
CREATE TABLE pageview_counts AS
  SELECT pageid, count(*) FROM pageviews
  WINDOW TUMBLING (size 5 second)
  GROUP BY pageid;
```	

