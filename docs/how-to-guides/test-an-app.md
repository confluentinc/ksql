---
layout: page
title: ksqlDB Testing Tool
tagline: Test your SQL statements in ksqlDB
description: Test your SQL statements in ksqlDB without the need for a full Kafka cluster
keywords: testing, qa, quality assurance, test runner
---

# How to test an application

!!! important
    ksqlDB 0.30 ships with a new sql-based testing tool (`run-ksql-test`). For documentation on the old test runner,
    switch to an older version of this page.

## Context

ksqlDB ships with a command line tool to test KSQL statements automatically.
It doesn't require an active {{ site.aktm }} or ksqlDB cluster.

## In action

```bash
run-ksql-test --test-directory path/to/tests --temp-folder path/to/temp/folder
```

## Usage

To test a set of KSQL statements, provide a folder containing the sql test files and another folder
that the testing tool will use to store temporary files.

```
NAME
        run-ksql-test - The KSQL SQL testing tool

SYNOPSIS
        run-ksql-test {--temp-folder | -tf} <tempFolder>
                      {--test-directory | -td} <testDirectory>

OPTIONS
        --temp-folder <tempFolder>, -tf <tempFolder>
            A folder to store temporary files

            This option may occur a maximum of 1 times


        --test-directory <testDirectory>, -td <testDirectory>
            A directory containing SQL files to test.

            This option may occur a maximum of 1 times
```

## File structure

Here is a sample test file:

```sql
--@test: Passing test

CREATE STREAM s (id INT KEY, foo INT) WITH (kafka_topic='s', value_format='JSON');
CREATE TABLE t (id INT PRIMARY KEY, bar INT) WITH (kafka_topic='t', value_format='JSON');

CREATE STREAM j AS SELECT s.id, s.foo, t.bar FROM s JOIN t ON s.id = t.id;

INSERT INTO t (rowtime, id, bar) VALUES (1, 1, 1);
INSERT INTO s (rowtime, id, foo) VALUES (1, 1, 2);

ASSERT VALUES j (rowtime, s_id, foo, bar) VALUES (1, 1, 2, 1);

--@test: Failing test
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])

SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2 FROM a;

INSERT INTO a (id, col1) VALUES (3, 5);

ASSERT VALUES b (id, col1) VALUES (3, 5);

CREATE OR REPLACE STREAM b AS SELECT id, col1 FROM a;
```

A test file contains one or more tests separated by the `--@test` directive.
Each test consists of comments containing directives and sql stataments.

### Directives

There are three directives available:

* `--@test: name of test`. Required in every test.
* `--@expected.error: error class`. Checks that the test throws an error of the
provided type. 
* `--@expected.message: error message`. Checks that the test throws an error with a
message containing the provided message. 

### Statements

`run-ksql-test` runs each statement in a test sequentially until an error is thrown or the end of
the test is reached.

The following are the supported KSQL statements in `run-ksql-test`:

- `CREATE STREAM`
- `CREATE TABLE`
- `CREATE STREAM AS SELECT`
- `CREATE TABLE AS SELECT`
- `ALTER STREAM`
- `ALTER TABLE`
- `DROP STREAM`
- `DROP TABLE`
- `SET`
- `UNSET`
- `INSERT INTO`
- `INSERT VALUES`

There are also four test only statements for verifying data.


#### ASSERT STREAM

```
ASSERT STREAM sourceName (tableElements)? (WITH tableProperties)?
```

Asserts the existence of a stream with the given elements and properties.

#### ASSERT TABLE

```
ASSERT TABLE sourceName (tableElements)? (WITH tableProperties)?
```

Asserts the existence of a table with the given elements and properties.

#### ASSERT VALUES

```
ASSERT VALUES sourceName (columns)? VALUES values
```

Asserts that a row with the given values is in a source.

#### ASSERT TOMBSTONES

```
ASSERT NULL VALUES sourceName (columns)? KEY values
```

Asserts that a tombstone is in a source.

### Running tests

`run-ksql-test` indicates the success or failure of a test by
printing the corresponding record. The following is the result of a
successful test:

```bash
run-ksql-test -td /path/to/test/directory -tf /path/to/temp/folder
```

Your output should resemble:

```
>>> Test passed!
>>> Test passed!
>>> Test passed!
```

Note that the tool may also write verbose log output to the terminal too, in 
which case you may need to page through it to locate the test status message.

If a test fails, the testing tool will indicate the failure along with
the cause. Here is an example of the output for a failing test:

```
>>>>> Test failed: Test failure for assert `ASSERT VALUES B (ID, COL1, COL2) VALUES (1, 1, 0)` (Line: 18, Col: 15):
Expected record does not match actual.
+--------------+--------------+--------------+--------------+--------------+
|.             |ROWTIME       |ID            |COL1          |COL2          |
+--------------+--------------+--------------+--------------+--------------+
|EXPECTED      |1             |1665202365011 |1             |0             |
|ACTUAL        |1             |1665202365003 |1             |1             |

file:///path/to/failing/test/file.sql:18
```

The tool will return an exit code of `1` if any test fails and `0` if all tests pass.

### Kafka cluster

`run-ksql-test` doesn't use a real Kafka cluster. Instead, it simulates
the behavior of a cluster with a single broker for the KSQL queries. This
means that the testing tool ignores configuration settings for the input
and output topics, like the number of partitions or replicas.
