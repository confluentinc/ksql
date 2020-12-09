---
layout: page
title: ksqlDB Concepts
tagline: Foundations of ksqlDB
description: Learn about ksqlDB under the hood.
keywords: ksqldb, architecture, collection, query, schema, window, view
---

<div class="cards">
  <div class="card concepts">
    <strong>Events</strong>
    <p class="card-body"><small>An event is the fundamental unit of data in stream processing.</small></p>
    <span><a href="/concepts/events">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Stream processing</strong>
    <p class="card-body"><small>Stream processing is a way to write programs computing over unbounded streams of events.</small></p>
    <span><a href="/concepts/stream-processing">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Materialized views</strong>
    <p class="card-body"><small>Materialized views precompute the results of queries at write-time so reads become predictably fast.</small></p>
    <span><a href="/concepts/materialized-views">Learn →</a></span>
  </div>
</div>

<div class="cards">
  <div class="card concepts">
    <strong>Streams</strong>
    <p class="card-body"><small>A stream is an immutable, append-only collection of events that represents a series of historical facts.</small></p>
    <span><a href="#">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Tables</strong>
    <p class="card-body"><small>A table is a mutable collection of events that models change over time.</small></p>
    <span><a href="#">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Queries</strong>
    <p class="card-body"><small>Queries are how you process events and retrieve computed results.</small></p>
    <span><a href="/concepts/queries">Learn →</a></span>
  </div>
</div>

<div class="cards">
  <div class="card concepts">
    <strong>Joins</strong>
    <p class="card-body"><small>Joins are how to combine data from many streams and tables into one.</small></p>
    <span><a href="/developer-guide/joins">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Time and windows</strong>
    <p class="card-body"><small>Windows help you bound a continuous stream of events into distinct time intervals.</small></p>
    <span><a href="/concepts/time-and-windows-in-ksqldb-queries">Learn →</a></span>
  </div>

  <div class="card concepts">
    <strong>Connectors</strong>
    <p class="card-body"><small>Connectors source and sink data from external systems.</small></p>
    <span><a href="/concepts/connectors">Learn →</a></span>
  </div>
</div>


<div class="cards">
  <div class="card concepts">
    <strong>Apache Kafka primer</strong>
    <p class="card-body"><small>None of this making sense? Take a step back and learn the basics of Kafka first.</small></p>
    <span><a href="/overview/apache-kafka-primer">Learn →</a></span>
  </div>
</div>