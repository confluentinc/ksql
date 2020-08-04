---
layout: page
title: How to structure data
tagline: Use DDL to structure data 
description: How to use DDL to structure data in ksqlDB
keywords: ksqldb, sql, ddl
---

##  Basics

- two collections: streams and tables
- what is a stream
  - kafka topic + schema
- what is a table
  - materialized stream
- both have columns
  - those columns have data types

- [ example ]

- How derivation works: you dont need to declare a new schema when you derive a new stream/table, its implicit.

## Constraints

- Limit the way that you can put data into a stream/table

### Primary key constraints

- Sole identifier for an entity with multiple rows
- Only used in tables

### Not null constraints

- Not implemented yet, point to GH issue

## Partition data

- Kafka keys control partitioning/sharding
- General requirements
- Different key formats

## Modify streams and tables

- Future work in 0.12