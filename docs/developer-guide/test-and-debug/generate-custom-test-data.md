---
layout: page
title: Generate Custom Test Data by Using the ksql-datagen Tool
tagline: Configure ksql-datagen with a custom schema
description: Learn how to produce data according to a schema that you define.
keywords: ksqldb, generate, data, event
---

Use the `ksql-datagen` command-line tool to generate test data that complies
with a custom schema that you define.

To generate test data, create an Apache Avro schema and pass it to
`ksql-datagen`. This generates random data according to the schema you
provide.

Also, you can generate data from a few simple, predefined schemas.

Prerequisites:

-   [Confluent Platform](https://docs.confluent.io/current/installation/installing_cp/index.html)
    is installed and running. This installation includes an {{ site.aktm }}
    broker, ksqlDB, {{ site.c3short }}, {{ site.zk }}, {{ site.sr }},
    {{ site.crest }}, and {{ site.kconnect }}.
-   If you installed {{ site.cp }} via TAR or ZIP, navigate to the
    installation directory. The paths and commands used throughout this
    tutorial assume that you're in this installation directory.
-   Java: Minimum version 1.8. Install Oracle Java JRE or JDK \>= 1.8 on
    your local machine.

The `ksql-datagen` tool is installed with {{ site.cp }} by default.

!!! note
	ksqlDB Server doesn't need to be running for `ksql-datagen` to generate
    records to a topic. The `ksql-datagen` tool isn't just for ksqlDB. You
    can use it to produce data to any Kafka topic that you have write access
    to.

Usage
-----

Use the following command to generate records from an Avro schema:

```bash
<path-to-confluent>/bin/ksql-datagen schema=<path-to-avro-file> format=<record format> topic=<kafka topic name> key=<name of key column> [options ...]
```

### Required Arguments

|                Name                | Default |                                                         Description                                                         
| ---------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------- 
| ``schema=<avro schema file>``      |         | Path to an Avro schema file. Requires the ``format``, ``topic``, and ``key`` options.                                       
| ``key-format=<key format>``        | Kafka   | Format of generated record keys: one of ``avro``, ``json``, ``delimited``, ``kafka``. Case-insensitive.                     
| ``value-format=<value format>``    | JSON    | Format of generated record values: one of ``avro``, ``json``, ``delimited``. Case-insensitive.                              
| ``topic=<kafka topic name>``       |         | Name of the topic that receives generated records                                                                           
| ``key=<name of key column>``       |         | Field to use as the key for generated records.                                                                              
| ``quickstart=<quickstart preset>`` |         | Generate records from a preset schema: ``orders``, ``users``, or ``pageviews``. Case-insensitive.                           
|                                    |         | If ``topic`` isn't specified, creates a topic named ``<preset>_kafka_topic_json``, for example, ``users_kafka_topic_json``. 

Use the following command to generate records from one of the predefined schemas:

```bash
<path-to-confluent>/bin/ksql-datagen quickstart=<quickstart preset> [options ...]
```

### Optional Arguments

The following options apply to both the `schema` and `quickstart` options.

|                     Name                     |                       Default                       |                                                              Description                                                              |                                                      |
| -------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| ``bootstrap-server=<kafka-server>:<port>``   | localhost:9092                                      | IP address and port for the Kafka server to connect to.                                                                               |                                                      |
| ``key-format=<key format>``                  | Kafka                                               | Format of generated record keys: ``avro``, ``json``, ``delimited`` or ``kafka``. Case-insensitive. Required by the ``schema`` option. |                                                      |
| ``value-format=<value format>``              | JSON                                                | Format of generated record values: ``avro``, ``json``, or ``delimited``.                                                              | Case-insensitive. Required by the ``schema`` option. |
| ``topic=<kafka topic name>``                 |                                                     | Name of the topic that receives generated records. Required by the ``schema`` option.                                                 |                                                      |
| ``key=<name of key column>``                 |                                                     | Field to use as the key for generated records. Required by the ``schema`` option.                                                     |                                                      |
| ``iterations=<number of records>``           | 1,000,000                                           | The maximum number of records to generate.                                                                                            |                                                      |
| ``msgRate=<rate to produce in msgs/second>`` | -1 (unlimited, i.e. as fast as possible)            | The rate to produce messages at, in messages-per-second.                                                                              |                                                      |
| ``propertiesFile=<path-to-properties-file>`` | ``<path-to-confluent>/etc/ksqldb/datagen.properties`` | Path to the ``ksql-datagen`` properties file.                                                                                         |                                                      |
| ``schemaRegistryUrl``                        | http://localhost:8081                               | URL of {{ site.sr }} when ``format`` is ``avro``.                                                                                     |                                                      |

!!! tip
	For usage information, enter `ksql-datagen help`.

Generate Records From a Predefined Schema
-----------------------------------------

The `ksql-datagen` tool provides some simple schemas for generating
example orders, users, and pageviews data.

### Generate Example Order Records With Structured Data

The `orders` quickstart option produces records that simulate orders,
with `orderid`, `itemid`, `price`, and `location` columns. The `location` column is
a STRUCT with `city`, `state`, and `zipcode` columns. The `orderId` column is stored
in the topic message key.

The following command generates example order records to a Kafka topic
named `orders_topic`:

```bash
<path-to-confluent>/ksql-datagen quickstart=orders topic=orders_topic
```

In the ksqlDB CLI or in {{ site.c3short }}, register a stream on
`orders_topic`:

```sql
CREATE STREAM orders_raw (
    orderId VARCHAR KEY,
    itemid VARCHAR,
    price DOUBLE,
    location STRUCT<
        city VARCHAR,
        state VARCHAR,
        zipcode INT>,
    timestamp VARCHAR
  ) WITH (
    KAFKA_TOPIC='orders_topic',
    VALUE_FORMAT='JSON'
  );
```

Inspect the schema of the `orders_raw` stream by using the DESCRIBE
statement:

```sql
DESCRIBE orders_raw;
```

Your output should resemble:

```
Name                 : ORDERS_RAW
 Field     | Type                                                                 
----------------------------------------------------------------------------------
 ORDERID   | VARCHAR(STRING)  (key)
 ITEMID    | VARCHAR(STRING)                                                      
 PRICE     | DOUBLE                                                               
 LOCATION  | STRUCT<CITY VARCHAR(STRING), STATE VARCHAR(STRING), ZIPCODE INTEGER> 
 TIMESTAMP | VARCHAR(STRING)                                                      
----------------------------------------------------------------------------------
```

For more information, see
[How to query structured data](../../how-to-guides/query-structured-data.md).

### Generate Example User Records

The `users` quickstart option produces records that simulate user data,
with `registertime`, `gender`, `regionid`, and `userid` fields. You can
join `userid` values with the page view records generated by the
`pageviews` quickstart option.

The following command generates example user records:

```bash
<path-to-confluent>/bin/ksql-datagen quickstart=users
```

In this example, no topic name is specified, so `ksql-datagen` creates a
topic named `users_kafka_topic_json`.

In the ksqlDB CLI or in {{ site.c3short }}, register a table on
`users_kafka_topic_json`:

```sql
CREATE TABLE users_original (
    userid VARCHAR PRIMARY KEY,
    registertime BIGINT,
    gender VARCHAR,
    regionid VARCHAR
  ) WITH (
    kafka_topic='users_kafka_topic_json',
    value_format='JSON'
  );
```

Inspect the schema of the `users_original` table by using the DESCRIBE
statement:

```sql
DESCRIBE users_original;
```

Your output should resemble:

```
Name                 : USERS_ORIGINAL
 Field        | Type                      
------------------------------------------
 USERID       | VARCHAR(STRING)  (key)
 REGISTERTIME | BIGINT                    
 GENDER       | VARCHAR(STRING)           
 REGIONID     | VARCHAR(STRING)           
------------------------------------------
```

### Generate Example User Records With Complex Data

The `users_` quickstart option produces records that simulate user data,
with `registertime`, `gender`, `regionid`, `userid`, `interests`, and
`contactInfo` columns. The `interests` column is an ARRAY, and the
`contactInfo` column is a MAP.

You can join `userid` values with the page view records generated by the
`pageviews` quickstart option.

The following command generates example user records that have complex
data:

```bash
<path-to-confluent>/bin/ksql-datagen quickstart=users_ topic=users_extended
```

In the ksqlDB CLI or in {{ site.c3short }}, register a table on
`users_extended`:

```sql
CREATE TABLE users_extended (
    userid VARCHAR PRIMARY KEY,
    registertime BIGINT,
    gender VARCHAR,
    regionid VARCHAR,
    interests ARRAY<STRING>,
    contactInfo MAP<STRING, STRING>
  ) WITH (
    kafka_topic='users_extended',
    value_format='JSON'
  );
```

Inspect the schema of the `users_extended` table by using the DESCRIBE
statement:

```sql
DESCRIBE users_extended;
```

Your output should resemble:

```
Name                 : USERS_EXTENDED
 Field        | Type                         
----------------------------------------------
 USERID       | VARCHAR(STRING)  (primary key)
 REGISTERTIME | BIGINT                       
 GENDER       | VARCHAR(STRING)              
 REGIONID     | VARCHAR(STRING)              
 INTERESTS    | ARRAY<VARCHAR(STRING)>       
 CONTACTINFO  | MAP<STRING, VARCHAR(STRING)> 
----------------------------------------------
```

For more information, see
[How to query structured data](../../how-to-guides/query-structured-data.md).

### Generate Example User Page Views

The `pageviews` quickstart option produces records that simulate page
views, with `viewtime`, `userid`, and `pageid` columns. You can join
`userid` values with the user records generated by the `users`
quickstart option.

The following command generates example pageview records to a Kafka
topic named `pageviews`:

```bash
<path-to-confluent>/bin/ksql-datagen quickstart=pageviews topic=pageviews
```

In the ksqlDB CLI or in {{ site.c3short }}, register a stream on
`pageviews`:

```sql
CREATE STREAM pageviews_original (
    viewtime bigint,
    userid varchar,
    pageid varchar
  ) WITH (
    kafka_topic='pageviews',
    value_format='DELIMITED'
  );
```

Inspect the schema of the `pageviews_original` stream by using the
DESCRIBE statement:

```sql
DESCRIBE pageviews_original;
```

Your output should resemble:

```
Name                 : PAGEVIEWS_ORIGINAL
 Field    | Type                      
--------------------------------------
 VIEWTIME | BIGINT                    
 USERID   | VARCHAR(STRING)           
 PAGEID   | VARCHAR(STRING)           
--------------------------------------
```

Generate Records From an Avro Schema
------------------------------------

### Define a Custom Schema

In this example, you download a custom Avro schema and generate matching
test data. The schema is named
[impressions.avro](https://github.com/apurvam/streams-prototyping/blob/master/src/main/resources/impressions.avro),
and it represents advertisements delivered to users.

Download `impressions.avro` and copy it to your home directory. It's
used by `ksql-datagen` when you start generating test data.

```bash
curl https://raw.githubusercontent.com/apurvam/streams-prototyping/master/src/main/resources/impressions.avro > impressions.avro
```

### Generate Test Data

When you have a custom schema registered, you can generate test data
that's made up of random values that satisfy the schema requirements.
In the `impressions` schema, advertisement identifiers are two-digit
random numbers between 10 and 99, as specified by the regular expression
`ad_[1-9][0-9]`.

Open a new command shell, and in the `<path-to-confluent>/bin`
directory, start generating test values by using the `ksql-datagen`
command. In this example, the schema file, `impressions.avro`, is in the
root directory.

```bash
<path-to-confluent>/bin/ksql-datagen schema=~/impressions.avro format=delimited topic=impressions key=impressionid
```

After a few startup messages, your output should resemble:

```
impression_796 --> ([ 1528756317023 | 'impression_796' | 'user_41' | 'ad_29' ])
impression_341 --> ([ 1528756317446 | 'impression_341' | 'user_34' | 'ad_32' ])
impression_419 --> ([ 1528756317869 | 'impression_419' | 'user_58' | 'ad_74' ])
impression_399 --> ([ 1528756318146 | 'impression_399' | 'user_32' | 'ad_78' ])
```

### Consume the Test Data Stream

In the ksqlDB CLI or in {{ site.c3short }}, register the `impressions`
stream:

```sql
CREATE STREAM impressions (viewtime BIGINT, key VARCHAR, userid VARCHAR, adid VARCHAR) WITH (KAFKA_TOPIC='impressions', VALUE_FORMAT='DELIMITED');
```

Create the `impressions2` persistent streaming query:

```sql
CREATE STREAM impressions2 as select * from impressions EMIT CHANGES;
```

Suggested Resources
-------------------

- [Podcast: Testing ksqlDB Applications ft. Viktor Gamov](https://developer.confluent.io/podcast/testing-ksqldb-applications-ft-viktor-gamov)
- [Easy Ways to Generate Test Data in Kafka](https://www.confluent.io/blog/easy-ways-generate-test-data-kafka/)
