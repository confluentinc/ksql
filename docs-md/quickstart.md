---
layout: page
title: ksqlDB Quick Start
tagline: Get Started with ksqlDB
description: Get started fast with ksqlDB quick starts and tutorials.
---

ksqlDB Quick Start
------------------

The [ksqlDB Quick Start](https://ksqldb.io/quickstart.html) is the easiest way to get
up and running with ksqlDB. It demonstrates a simple workflow that creates 
an event stream on an {{ site.aktm }} topic and uses SQL statements to write
streaming queries against events in {{ site.ak }}.

The quick start shows how to:

1. Get ksqlDB
2. Start ksqlDB Server
3. Start ksqlDB interactive CLI
4. Create a stream
5. Run a continuous query over the stream
6. Populate the stream with events

ksqlDB Tutorials and Examples
-----------------------------

The [ksqlDB tutorials and examples](tutorials/index.md)
page provides introductory and advanced ksqlDB usage scenarios in both
local and Docker-based versions.

- The [ksqlDB Examples](tutorials/examples.md) show commonly used SQL queries
  and statements used in ksqlDB applications.
- [Clickstream Data Analysis Pipeline Using ksqlDB (Docker)](tutorials/clickstream-docker.md)
  Shows how to use ksqlDB, ElasticSearch, and Grafana to analyze
  data feeds and build a real-time dashboard for reporting and
  alerting.

Page last revised on: {{ git_revision_date }}
