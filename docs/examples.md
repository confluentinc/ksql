# Examples

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | Examples | [FAQ](/docs/faq.md#frequently-asked-questions)  | [Roadmap](/docs/roadmap.md#roadmap) | [Demo](/docs/demo.md#demo) |
|---|----|-----|----|----|----|----|----|

 

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
 in the stream, we can provide such information in the WITH clause. For instance, if the kafka
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

Now that we have the 'pageviews_original' stream and 'users_original' table, let's see some
example queries that you can write in KSQL. We focus on two types of KSQL statements, CREATE
STREAM AS SELECT and CREATE TABLE AS SELECT. For these statements KSQL
persists the results of the query in a new stream or table which is backed by a Kafka topic.
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

The following query creates a new stream by joining pageview_transformed stream with
the users_original table:

```sql
 CREATE STREAM pageview_enriched AS SELECT viewtime, pv.userid, pageid , timestring,
  gender, regionid, userid, interests, contactinfo FROM
 pageview_transformed pv LEFT JOIN users_original ON pv.userid = users_original.userid;

```

### Window Aggregat

Now let's assume we want to count the number of pageviews per region. Here is the query that
would perform this count.


```sql
 CREATE TABLE pageview_per_region AS SELECT regionid, count(*) FROM
 pageview_enriched pv GROUP BY regionid;

```

The above query counts the pageview from the query start time until we terminate the query. Note
that we used CREATE TABLE AS SELECT statement here since the result of the query is a KSQL table.
 The results of aggregate queries in KSQL are always a table since we compute the aggregate for
 each key, and possibily window and update these results as we process new tuples.
KSQL supports aggregation over WINDOW too. Let's rewrite the above query so that we compute the
pageview count per region every 1 minute.

```sql
 CREATE TABLE pageview_per_region_perminute AS SELECT regionid, count(*) FROM
 pageview_enriched pv WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY regionid;

```

If we want to count the pageviews for only Region_6 from the female users for every 30 seconds we
 can change the above query as the following:

 ```sql
  CREATE TABLE pageview_per_region_per30sec AS SELECT regionid, count(*) FROM pageview_enriched pv
  WINDOW TUMBLING (SIZE 30 SECONDS) WHERE UCASE(gender)='FEMALE' AND LCASE(regionid)='region_6' GROUP BY regionid;

 ```

As you can see we used UCASE and LCASE functions in KSQL to convert the values of gender and
regionid columns to upper and lower case respectively so we can match them correctly.

KSQL supports HOPPING and SESSION windows too. The following query is the same query as above
that computes the count for hopping window of 30 seconds that advances by 10 seconds:

 ```sql
  CREATE TABLE pageview_per_region_per30sec AS SELECT regionid, count(*) FROM pageview_enriched pv
  WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS) WHERE UCASE(gender)='FEMALE' AND LCASE
  (regionid)='region_6'
  GROUP BY regionid;

 ```

The following queries show how to access array items and map values in KSQL. The 'interest'
column in the user table is an array of string with size two that represents the first and second
 interest of each user. The contactinfo column is a string to string map that represents the
 following contact information for each user: phone, city, state and zipcode.  The following
 query will create a new stream from pageview_enriched that includes the first interrest of each
 user along with the city and zipcode for each user.

 ```sql
   CREATE STREAM pageview_interest_contact AS SELECT interests[0] as firstinterest,
   contactinfo['zipcode'] as zipcode, contactinfo['city'] as city, viewtime, PV_USERID,
   pageid , timestring, gender, regionid FROM pageview_enriched;

  ```
We can use the newly created pageview_interest_contact stream and count the number of pageview
from each city for window sessions with session inactivity gap of 60 seconds.

 ```sql
   CREATE TABLE pageview_count_city_session AS SELECT city, count(*) FROM pageview_interest_contact
   WINDOW SESSION (60 SECONDS) GROUP BY city;

  ```
