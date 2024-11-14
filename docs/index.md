---
layout: page
title: ksqlDB Overview
tagline: What is ksqlDB?
description: Learn about ksqlDB, the database for creating stream processing applications with Apache Kafka®.
keywords: ksqldb
---

[ksqlDB](https://ksqldb.io/) is a database purpose-built
for stream processing applications on top of {{ site.aktm }}.

[Get Started](https://ksqldb.io/quickstart.html){ .md-button .md-button--top-level }

!!! tip

    The documentation on this site applies to ksqlDB Standalone,
    {{ site.ccloud }} ksqlDB, and {{ site.cp }} ksqlDB. For more
    information about differences in ksqlDB on {{ site.ccloud }} and
    {{ site.cp }} see:

    - [Confluent Cloud ksqlDB](https://docs.confluent.io/cloud/current/ksqldb/overview.html)
    - [Confluent Platform ksqlDB](https://docs.confluent.io/platform/current/ksqldb/index.html)  

## Try it

<div class="cards">
  <div class="card getting-started">
    <strong>Quickstarts</strong>
    <ul class="card-items">
      <li><a href="https://ksqldb.io/quickstart.html">With Standalone</a></li>
      <li><a href="https://ksqldb.io/quickstart-cloud.html">With Confluent Cloud</a></li>
      <li><a href="https://ksqldb.io/quickstart-platform.html">With Confluent Platform</a></li>
    </ul>
  </div>

  <div class="card getting-started">
    <strong>How-to guides</strong>
    <ul class="card-items">
      <li><a href="/how-to-guides/query-structured-data/">Structured data</a></li>
      <li><a href="/how-to-guides/use-connector-management/">Connector management</a></li>
      <li><a href="/how-to-guides/create-a-user-defined-function/">User-defined functions</a></li>
      <li><a href="/how-to-guides/substitute-variables/">Variables</a></li>
    </ul>
    <small><a href="/how-to-guides/" class="card-more">More →</a></small>
  </div>

  <div class="card getting-started">
    <strong>Tutorials</strong>
    <ul class="card-items">
      <li><a href="/tutorials/materialized/">Materialized cache</a></li>
      <li><a href="/tutorials/etl/">Streaming ETL pipeline</a></li>
      <li><a href="/tutorials/event-driven-microservice/">Event-driven microservice</a></li>
    </ul>
    <small><a href="/tutorials/" class="card-more">More →</a></small>
  </div>
</div>

## Learn

<div class="cards">
  <div class="card getting-started">
    <strong>How it works</strong>
    <ul class="card-items">
      <li><a href="https://www.confluent.io/blog/how-real-time-stream-processing-works-with-ksqldb/">SQL Stream processing</a></li>
      <li><a href="https://www.confluent.io/blog/how-real-time-materialized-views-work-with-ksqldb/">Materialized views</a></li>
      <li><a href="https://www.confluent.io/blog/how-real-time-stream-processing-safely-scales-with-ksqldb/">Scaling and fault tolerance</a></li>
    </ul>
  </div>

  <div class="card getting-started">
    <strong>Concepts</strong>
    <ul class="card-items">
      <li><a href="/concepts/apache-kafka-primer/">Apache Kafka primer</a></li>
      <li><a href="/concepts/stream-processing/">Stream processing</a></li>
      <li><a href="/concepts/time-and-windows-in-ksqldb-queries/">Time and windowing</a></li>
      <li><a href="/concepts/connectors/">Connectors</a></li>
    </ul>
    <small><a href="/concepts/" class="card-more">More →</a></small>
  </div>

  <div class="card getting-started">
    <strong>Case studies</strong>
    <ul class="card-items">
      <li><a href="https://www.confluent.io/blog/real-time-data-replication-with-ksqldb/">Real-time data replication</a></li>
      <li><a href="https://www.confluent.io/blog/how-pushowl-uses-ksqldb-to-scale-analytics-and-reporting-use-cases/">Real-time analytics</a></li>
      <li><a href="https://www.confluent.io/blog/real-time-business-intelligence-using-ksqldb">Business intelligence</a></li>
      <li><a href="https://www.confluent.io/blog/broadcom-uses-ksqldb-to-modernize-machine-learning-anomaly-detection/">Anomaly detection</a></li>
    </ul>
    <small><a href="https://ksqldb.io/news-and-community.html" class="card-more">More →</a></small>
  </div>

</div>

## Watch

<div class="cards">
  <div class="card getting-started">
    <strong>Intros</strong>
    <ul class="card-items">
      <li><a href="https://www.youtube.com/watch?v=-kFU6mCnOFw">Stream processing fundamentals</a></li>
      <li><a href="https://www.youtube.com/watch?v=SHKjuN2iXyk">Ask Confluent, ksqlDB overview</a></li>
      <li><a href="https://www.youtube.com/watch?v=7mGBxG2NhVQ">An introduction to ksqlDB</a></li>
    </ul>
  </div>

  <div class="card getting-started">
    <strong>Demos</strong>
    <ul class="card-items">
      <li><a href="https://www.youtube.com/watch?v=D5QMqapzX8o">Building a movie rating app</a></li>
      <li><a href="https://www.youtube.com/watch?v=4odZGWl-yZo">Stream data from AWS to Azure</a></li>
      <li><a href="https://www.youtube.com/watch?v=ad02yDTAZx0">Transform and sink events to JDBC</a></li>
      <li><a href="https://www.youtube.com/watch?v=2fUOi9wJPhk&ab_channel=RobinMoffatt">Building a streaming data pipeline</a></li>
    </ul>
  </div>

</div>

<div class="cards">

  <div class="card getting-started">
    <strong>Courses</strong>
    <ul class="card-items">
      <li><a href="https://developer.confluent.io/learn-kafka/ksqldb/intro/">ksqlDB 101</a></li>
      <li><a href="https://developer.confluent.io/learn-kafka/inside-ksqldb/streaming-architecture/">Inside ksqlDB</a></li>
    </ul>
    <small><a href="https://developer.confluent.io/" class="card-more">More →</a></small>
  </div>

  <div class="card getting-started how-to-videos">
    <strong>How to</strong>
    <ul class="card-items">
      <li><a href="https://www.youtube.com/watch?v=scpbbl71CD8&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=1&ab_channel=RobinMoffatt">Handle time</a></li>
      <li><a href="https://www.youtube.com/watch?v=5NoU7D4OGA0&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=2&ab_channel=RobinMoffatt">Split and merge streams</a></li>
      <li><a href="https://www.youtube.com/watch?v=sLAztA-rt74&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=3&ab_channel=RobinMoffatt">Change serialization formats</a></li>
      <li><a href="https://www.youtube.com/watch?v=MLSrnBTSGlQ&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=4&ab_channel=RobinMoffatt">Connect to external systems</a></li>
      <li><a href="https://www.youtube.com/watch?v=_-j7aKE0kl0&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=5&ab_channel=RobinMoffatt">Create a stateful table</a></li>
      <li><a href="https://www.youtube.com/watch?v=_0Ktp2eB-as&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=6&ab_channel=RobinMoffatt">Join streams and tables</a></li>
      <li><a href="https://www.youtube.com/watch?v=7pH5KEQiYYo&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=7&ab_channel=RobinMoffatt">Manipulate schemas</a></li>
      <li><a href="https://www.youtube.com/watch?v=TfX70zBHyPM&list=PL5T99fPsK7pqrn7Ff-k4wdoZFlCH0EGC1&index=8&ab_channel=RobinMoffatt">Filter rows</a></li>
    </ul>
  </div>
</div>