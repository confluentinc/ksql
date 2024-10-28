---
layout: page
title: ksqlDB Concepts
tagline: Foundations of ksqlDB
description: Learn about ksqlDB under the hood.
keywords: ksqldb, architecture, collection, query, schema, window, view
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/concepts/index.html';
</script>

Learn the core concepts that ksqlDB is built around.

<div class="cards">
  <div class="card concepts">
    <a href="/concepts/events"><strong>Events</strong></a>
    <p class="card-body"><small>An event is the fundamental unit of data in stream processing.</small></p>
    <span><a href="/concepts/events">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/stream-processing"><strong>Stream processing</strong></a>
    <p class="card-body"><small>Stream processing is a way to write programs computing over unbounded streams of events.</small></p>
    <span><a href="/concepts/stream-processing">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/materialized-views"><strong>Materialized views</strong></a>
    <p class="card-body"><small>Materialized views precompute the results of queries at write-time so reads become predictably fast.</small></p>
    <span><a href="/concepts/materialized-views">Learn →</a></span>
  </div>
</div>

<div class="cards">
  <div class="card concepts">
    <a href="/concepts/streams"><strong>Streams</strong></a>
    <p class="card-body"><small>A stream is an immutable, append-only collection of events that represents a series of historical facts.</small></p>
    <span><a href="/concepts/streams">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/tables"><strong>Tables</strong></a>
    <p class="card-body"><small>A table is a mutable collection of events that models change over time.</small></p>
    <span><a href="/concepts/tables">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/queries"><strong>Queries</strong></a>
    <p class="card-body"><small>Queries are how you process events and retrieve computed results.</small></p>
    <span><a href="/concepts/queries">Learn →</a></span>
  </div>
</div>

<div class="cards">
  <div class="card concepts">
    <a href="/developer-guide/joins"><strong>Joins</strong></a>
    <p class="card-body"><small>Joins are how to combine data from many streams and tables into one.</small></p>
    <span><a href="/developer-guide/joins">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/time-and-windows-in-ksqldb-queries"><strong>Time and windows</strong></a>
    <p class="card-body"><small>Windows help you bound a continuous stream of events into distinct time intervals.</small></p>
    <span><a href="/concepts/time-and-windows-in-ksqldb-queries">Learn →</a></span>
  </div>

  <div class="card concepts">
    <a href="/concepts/functions"><strong>User-defined functions</strong></a>
    <p class="card-body"><small>Extend ksqlDB to invoke custom code written in Java.</small></p>
    <span><a href="/concepts/functions">Learn →</a></span>
  </div>
</div>

<div class="cards">
  <div class="card concepts">
    <a href="/concepts/connectors"><strong>Connectors</strong></a>
    <p class="card-body"><small>Connectors source and sink data from external systems.</small></p>
    <span><a href="/concepts/connectors">Learn →</a></span>
  </div>
  <div class="card concepts">
    <strong>Lambda Functions</strong>
    <p class="card-body"><small>Lambda functions allow you to apply in-line functions without creating a full UDF.</small></p>
    <span><a href="/concepts/lambda-functions">Learn →</a></span>
  </div>
  
  <div class="card concepts">
    <a href="/overview/apache-kafka-primer"><strong>Apache Kafka primer</strong></a>
    <p class="card-body"><small>None of this making sense? Take a step back and learn the basics of Kafka first.</small></p>
    <span><a href="/overview/apache-kafka-primer">Learn →</a></span>
  </div>
</div>