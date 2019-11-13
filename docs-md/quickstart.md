---
layout: page
title: ksqlDB Quick Start
tagline: Get Started with ksqlDB
description: Get started fast with ksqlDB quick starts and tutorials.
---

{{ site.cp }} Quick Start
-------------------------

The [Confluent Platform Quick Start](https://docs.confluent.io/current/quickstart/index.html)
is the easiest way to get you up and running with {{ site.cp }} and
ksqlDB. It will demonstrate a simple workflow with topic management,
monitoring, and using ksqlDB to write streaming queries against data
in {{ site.aktm }}.

ksqlDB Tutorials and Examples
-----------------------------

The [ksqlDB tutorials and examples](tutorials/index.md)
page provides introductory and advanced ksqlDB usage scenarios in both
local and Docker-based versions.

- [Writing Streaming Queries Against Kafka Using ksqlDB](tutorials/index.md).
This tutorial demonstrates a simple workflow using ksqlDB to write
streaming queries against messages in {{ site.ak }}.
- [Clickstream Data Analysis Pipeline Using ksqlDB (Docker)](tutorials/clickstream-docker.md).
Learn how to use ksqlDB, ElasticSearch, and Grafana to analyze
data feeds and build a real-time dashboard for reporting and
alerting.

You can configure Java streams applications to deserialize and
ingest data in multiple ways, including {{ site.ak }} console
producers, JDBC source connectors, and Java client producers. For
full code examples,
[connect-streams-pipeline](https://github.com/confluentinc/examples/tree/master/connect-streams-pipeline).

Page last revised on: {{ git_revision_date }}
