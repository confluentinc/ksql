- DDL: how to structure data

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

## Partitioning

- Kafka keys control partitioning/sharding
- General requirements
- Different key formats

## Modifying streams and tables

- Future work in 0.12