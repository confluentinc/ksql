# KSQL - a Streaming SQL Engine for Apache Kafka™ from Confluent
---

A DEVELOPER PREVIEW

---

KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL commands. 

KSQL does not require proficiency with a programming language such as Java or Go, and it does not require you to install and manage a separate processing cluster technology. As such, it opens up the world of stream processing to a broader set of users and applications than ever before.

This release is a DEVELOPER PREVIEW which is free and open-source from Confluent under the Apache 2.0 license.

---

# Hello, KSQL!
---
Here are some example queries to illustrate the look and feel of the KSQL syntax:

Filter an inbound stream of page views to only show errors

```sql
SELECT STREAM request, ip, status 
 WHERE status >= 400
```

Create a new stream that contains the pageviews from female users only
```sql
CREATE STREAM pageviews_by_female_users AS
  SELECT users.userid AS userid, pageid, regionid, gender FROM pageviews
  LEFT JOIN users ON pageview.userid = users.userid
  WHERE gender = 'FEMALE';
```

Continuously compute the number of pageviews for each page with 5-second tumbling windows
```sql
CREATE TABLE pageview_counts AS
  SELECT pageid, count(*) FROM pageviews
  WINDOW TUMBLING (size 5 second)
  GROUP BY pageid;
```

# Let’s Play with KSQL
---

* First-time users may want to try our [interactive quickstart](https://github.com/confluentinc/ksql).
* If you want a more realistic end-to-end example, walk through our [KSQL demo](https://github.com/confluentinc/ksql).

To learn more about KSQL see our [documentation](https://github.com/confluentinc/ksql) including the [KSQL Syntax Guide](https://github.com/confluentinc/ksql).

# Need help?
---
If you need help or have questions, you have several options:
* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://confluent.typeform.com/to/GxTHUD). Account registration is free and self-service.
* Create a [ticket](https://github.com/confluentinc/ksql) in our issue [tracker](https://github.com/confluentinc/ksql).
* Join the [Confluent google group](https://groups.google.com/forum/#!forum/confluent-platform).

# How it works
---
KSQL consists of a client and a server component. The client is a command line interface (CLI) similar to the CLIs of MySQL or PostgreSQL. The server, of which you can run one or many instances, executes those queries for you.

You can use KSQL in stand-alone mode and/or in client-server mode.

In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

In client-server mode, a pool of KSQL server(s) can be running on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

# Frequently Asked Questions
---
**What are the benefits of KSQL?**

KSQL allows you to query, read, write, and process data in Apache Kafka in real-time and at scale using intuitive SQL-like syntax. KSQL does not require proficiency with a programming language such as Java or Scala, and you don’t have to install a separate processing cluster technology.

**What are the technical requirements of KSQL?**

KSQL only requires 
1. a Java runtime environment
2. access to an Apache Kafka cluster for reading and writing data in real-time. The cluster can be on-premises or in the cloud.

We recommend the use of [Confluent Platform](https://www.confluent.io/product/confluent-platform/) or [Confluent Cloud](https://www.confluent.io/confluent-cloud/) for running Apache Kafka.

**Is KSQL owned by the Apache Software Foundation?**

No, KSQL is owned and maintained by [Confluent Inc.](https://www.confluent.io/) as part of its free [Confluent Open Source](https://www.confluent.io/product/confluent-open-source/) product.

**How does KSQL compare to Apache Kafka’s Streams API?**

KSQL is complementary to the Kafka Streams API, and indeed executes queries through Streams applications. One of the key benefits of KSQL is that it does not require the user to develop any code in Java or Scala. 
This enables users to use a SQL-like interface alone to construct streaming ETL pipelines, as well as responding to a real-time, continuous business requests. For full-fledged stream processing applications Kafka Streams remains a more appropriate choice.
As with many technologies each has its sweet-spot based on technical requirements, mission-criticality, and user skillset.

**Is KSQL ready for production?**

KSQL is a technical preview at this point in time.  We do not yet recommend its use for production purposes.

**Can I use KSQL with my favorite data format (e.g. JSON, Avro)?**

KSQL currently supports formats: 

* DELIMITED (e.g. CSV)
* JSON

_Support for AVRO is expected soon._

**Is KSQL fully compliant to ANSI SQL?**

KSQL is a dialect inspired by ANSI SQL. It has several differences because it is geared at processing streaming data. For example, ANSI SQL has no notion of “windowing” for use cases such as performing aggregations on data grouped into 5-minute windows, which is a commonly required functionality in the streaming world.

# Contributing to KSQL
---
*This section contains information about how to contribute code and documentation, etc.*

To build KSQL locally:

```sh
$ git clone https://github.com/confluentinc/ksql.git
$ cd ksql
$ mvn clean package
```

# License
---
The project is licensed under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/)*

