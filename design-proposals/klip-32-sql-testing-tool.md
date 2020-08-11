# KLIP 32 - A SQL Testing  Tool

**Author**: agavra | 
**Release Target**: TBD | 
**Status**: _Discussion_ | 
**Discussion**: TBD

**tl;dr:** _Introduce **Y**et **A**nother **T**esting **T**ool (YATT) that is written and driven 
primarily by SQL-based syntax to add coverage for test cases that require interleaving statements
and inserts. Eventually, the goal is to replace the external facing ksql-test-runner and maybe 
(R)QTT tests._
           
## Motivation and background

Our existing testing infrastructure is powerful and can handle many of our existing use cases. It,
however, lacks the flexibility to be useful when testing query upgrades: neither framework supports
interleaving statements/inserts/asserts. This KLIP proposes a new, imperative, testing tool to
cover that use case and describes a way to replace the existing tools with this functionality.

The list below enumerates the motivating design principles:

1. the tool should support imperative tests, allowing interleaved statements/inserts/asserts
1. the tool should allow tests to be written primarily in SQL, avoiding special-syntax directives
    when possible
1. the tool should leverage as much of the ksqlDB engine as possible, keeping testing-specific
    code to a minimum. Whenever we require new functionality for the testing tool, we will first
    consider supporting it directly in ksqlDB.
1. the tool should run many tests quickly, with the ability to parallelize test runs

Another minor benefit of a SQL-based testing tool is to allow inline comments describing statements
and asserts.

### Why Not Reuse?

Why introduce YATT (Yet Another Testing Tool) when we already have three existing options? There
are a few factors here:

- the primary motivation is practical in nature: We need interleaved testing for the query upgrades 
    work, and building this into QTT would require essentially an entire rewrite of both the existing
    tool and the historical test execution while maintaining or migrating all the historical plans. 
    In order to deliver query upgrades (see KLIP-28) quickly, we will begin work on YATT while 
    maintaining support for QTT tests.
- the ksql-test-runner and (R)QTT have, over time, diverged significantly from the ksqlDB engine and 
    require lots of custom code (the ksqldb-functional-test module has 11k lines of Java). 
    Specifically, the custom JSON format requires maintaining a parallel, testing-only framework and 
    serde to convert inputs/outputs to their desired serialization format. Instead, we should 
    leverage the production SQL expression functionality to produce data.
- providing a SQL-driven testing tool will mesh better with our product offering, allowing users to
    write tests without every leaving the "SQL mindset"
    
## Scope

This KLIP covers:

- design of the test file format
- design of the testing-specific directives
- outline features that need to be implemented in order to replace (R)QTT/testing-tool

This KLIP does not cover:

- implementation of a migration path from (R)QTT/testing-tool, but we plan to deprecate and remove
    the ksql-test-runner over the next few releases. At the time when we remove it, we will build
    some convenience scripts to help generate YATT sql files.
- generating historical plans from YATT sql tests

The intention of this KLIP is to provide a solid foundation for YATT to eventually replace the other
testing tools and to make sure we are working towards a cleaner code base instead of littering
it with testing code that needs to be maintained and will eventually slow down our development
velocity. That being said, it does not cover all the details required, or provide a timeline, to
migrate the existing tools.

## Design

The best way to describe the testing tool is by a motivating example, such as the test file below:

```sql
---------------------------------------------------------------------------------------------------
--@test: dml - stream - add filter
---------------------------------------------------------------------------------------------------

CREATE STREAM foo (id VARCHAR KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, '1', 1);

ASSERT DATA bar OFFSET 1 (rowtime, id, col1) VALUES (1, '1', 1);

CREATE OR REPLACE STREAM bar AS SELECT * FROM foo WHERE col1 = 123;

INSERT INTO foo (rowtime, id, col1) VALUES (2, '2', 2);
INSERT INTO foo (rowtime, id, col1) VALUES (3, '3', 123);

ASSERT DATA bar OFFSET 2 (rowtime, id, col1) VALUES (3, '3', 123);
ASSERT STREAM bar (id VARCHAR KEY, col1 INT) WITH (kafka_topic='BAR', value_format='JSON');

---------------------------------------------------------------------------------------------------
--@test: dml - stream - change column
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: "Cannot REPLACE data source: DataSource '`BAR`' has schema ..."
---------------------------------------------------------------------------------------------------

CREATE STREAM foo (id VARCHAR KEY, col1 VARCHAR, col2 VARCHAR) 
    WITH (kafka_topic='foo', value_format='JSON');

-- the below operation is invalid because "col2" will be missing from the
-- schema after renaming it to col3
CREATE STREAM bar AS SELECT id, col1, col2 FROM foo;
CREATE OR REPLACE STREAM bar AS SELECT id, col1, col2 AS col3 FROM foo;
```

### Test Structure

YATT will accept as a parameter a directory containing testing files, and will run all the
tests in the directory in a single JVM. Alternatively, it can accept a single test file to
run just a limited subset of tests as well as which tests to run within the file based on
regex.

Each test file can contain one or more tests, separated by the `--@test` directive. This will
improve on a pain point of the existing ksql-test-runner, which requires three files for a single
test. The test name is a concatenation of the test file time and the contents of the `--@test`
directive.

Then, the test can contain any number of interleaved statements that are supported by the testing
tool. Data will be inserted into topics using `INSERT INTO` and asserts will be executed using the
new `ASSERT` syntax (described below).

There will be additional "meta" directives that will allow for testing functionality like expected
exceptions.

### The `ASSERT` Statement

`ASSERT` is the primary way to ensure conditions in YATT, and will be parsed and implemented 
using ksqlDB's `AstBuilder` to make sure YATT can leverage the existing expression parsing support. 

At first, `ASSERT` statements will fail if they are sent to a production server, but we may consider
allowing `ASSERT` in the future in order to allow REPL-based testing/experimentation.

#### `ASSERT DATA`

The `ASSERT DATA` statement asserts that data exists at an optionally provided offset (or otherwise 
sequential based on the last `ASSERT DATA`) with the given values. If the `ASSERT DATA` does not
specify certain columns they will be ignored (allowing users to assert only subset of the columns
match what they expect).

The values will be created in the same way that `INSERT INTO` creates values:
```sql
ASSERT DATA source_name [OFFSET at_offset] ( { column_name } [, ...] ) VALUES ( value [, ...] );
```

The `ASSERT DATA` statement will allow users to specify psuedocolumns as well, such as `ROWTIME`,
and when ksqlDB supports constructs such as `PARTITION` and/or headers, YATT will inherit these
psuedocolumns as well.

#### `ASSERT NO DATA`

The `ASSERT NO DATA` statement will allow users to ensure that no more data exists in a specified
source:
```sql
ASSERT NO DATA source_name [OFFSET from_offset];
```

#### `ASSERT SOURCE`

The `ASSERT (TABLE | STREAM)` statement asserts that the given stream or table has the specified
columns and physical properties.
```sql
ASSERT (STREAM | TABLE) source_name ( { column_name data_type [[PRIMARY] KEY] } [, ...] )
    WITH (property_name = expression [, ...] );
```

#### `ASSERT TYPE`

The `ASSERT TYPE` statement ensures that a custom type has the expected type. This is especially
useful when chaining multiple `CREATE TYPE` statements together and asserting the types later
in the chain are correct.
```sql
ASSERT TYPE type_name AS type;
```

#### Considered Alternative

It is possible to use a meta directive (see below) in order to drive asserts as well. This was
considered and rejected because it would require additional parsing and more testing-specific code
to convert to expressions and pass into the codegen, which diverges from the motivating principle 
of minimizing testing-only code.

### Meta Directives

Some functionality cannot be addressed in standard SQL syntax and will instead be supported using
meta directives. Some examples of these directives:

- The `--@expected.error` and `--@expected.message` directives will indicate that the below test 
    expects an exception to be thrown with the specified message and type.
- The `--@topic.denylist` directive will ensure that any topics matching the denylist will not
    be present at the end of the test execution.

### Global Checks

These are a set of checks that will happen without any directive or `ASSERT` statement. Some
checks that we may consider including:

- _Processing Log Check_: this check will ensure that there are no failures in the processing log.
    If the test case intends to check that the processing log contains certain entries, this check
    can be disabled on a test-by-test basis and assert via `ASSERT DATA` on the processing log
    stream.
    
### Language Gaps

There are some features that we need to implement in ksqlDB's SQL language in order to put YATT
on par with the existing testing tools. Some of those are outlined here:

- _Windowed Keys_: when we have structured key support, we can `ASSERT DATA` and just provide the
    struct as the value. Until then, windowed keys will either be unsupported or stringified.
- _Tombstones_: we will implement `DELETE FROM table WHERE key = value` syntax to allow tombstones
    to be inserted into tables, both for ksqlDB and YATT.
- _Null Values for Streams_: insert `null` values into streams will not be supported for the first
    iteration of YATT. Eventually, if we want to support this, we can add a directive like
    `--@null.value: topic key` to produce a `null` valued record into a specific topic/source.
- _Other Unsupported Inserts_: there are other types of inserts that ksqlDB doesn't allow for at the
    moment (e.g. `enum` support or binary formats). This is only somewhat a regression from the 
    existing (R)QTT tests, so we may or may not decide to support it in YATT.
    
### Error Reporting & UX

One of the bigger concerns with the existing ksql-test-runner is that it's helpful in letting
us know _that_ an error happened, but not so much _why_ it happened or even at times _what_
exactly happened. To avoid this pitfall, the errors will include:

- the assert statement that failed, with a link to the line that failed in the test file
- the actual data, and the expected data
- the ability to add `PRINT` statements to debug the issue further by printing topic contents

When data comparisons fail, we will leverage our SQL formatters to display the errors the
same way we display `SELECT` statements - in a tabular, easy-to-digest way. YATT will alternatively
take in a flag that allows failure messages to be output as machine-readable JSON, and this
will leverage our existing REST API JSON formats and be helpful for users reporting test failures
programmatically via some CI pipeline.

### CI/CD Implications

Most users that will be using this testing tool as part of a CI/CD pipeline will want to run
_exactly_ the scripts that are being deployed. To support this, the `RUN SCRIPT` language feature
will be supported and can be interleaved just like any other statement in the testing tool. This
is especially helpful when testing query upgrades and schema evolution. 

In this use case below, we can imagine that the user has a directory structure containing
all changes to their production cluster:

```
+ dir
|--+ 1
|    |--- 1_create_foo_bar.sql
|    |--- 1_test.sql
|
|--+ 2
|    |--- 2_update_bar.sql
|    |--- 2_test.sql
```

Then they write the following files, and ask the tool to recursively look through the top-level 
directory and run all tests. This test utilizes some syntax (`--@depends`) for demonstration of
how we could use this in a CI/CD pipeline to chain tests together as well, pairing tests with
their corresponding production sql files.

```sql
-- 1_create_foo_bar.sql
CREATE STREAM foo (id INT KEY, col1 VARCHAR) WITH (...)
CREATE STREAM bar AS SELECT * from FOO;
```

```sql
-- 1_test.sql
--@test: 1_test
RUN SCRIPT '1_create_foo_bar.sql`;
ASSERT STREAM foo (...) WITH (...);
INSERT INTO foo ...;
```

```sql
-- 2_update_bar.sql
CREATE OR REPLACE STREAM bar AS SELECT * FROM foo WHERE foo.id > 0;
```

```sql
--@test: 2_test
--@depends: 1/1_test.sql
RUN_SCRIPT '2_update_bar.sql';
INSERT INTO foo ...;
ASSERT DATA bar ...;
```



### Implementation

There are two ways of implementing such a testing tool:

1. backed by multiple topology test drivers, one for each query
1. backed by a real Kafka cluster

The initial implementation will be scoped to just the former implementation in order to ensure
speedy test execution. Since YATT will help drive development, it is necessary that the tests run
quickly - both in batch and as single test cases. 

### Extensions

There are some extensions to YATT that we may want to consider. I'm listing them here
because I'm currently in the mindset of thinking about them, and I don't want to forget!

- _BYO Kafka_: we may want to allow users to "plug in their own kafka" cluster and run these tests
    against a real Kafka cluster. This would allow users to produce whatever data they want
    outside of this tool, mitigating the limitations described above. It also would allow them
    to "debug" further when something doesn't go the way they want by examining the input/output
    topics through ksqlDB.
- _YaaS_: (YAAT as a Service) we may want to consider a deployment option where YAAT just watches
    a directory in some long-running deployment, running new tests whenever a directory changes.

## Test Plan

This is a testing tool! It is self-testing in the sense that we will be using it to test query upgrades
and assert that it works the way that it is expected to. We will also make sure to have negative tests
to ensure that it should fail when it is supposed to fail.

## Delivery Milestones

1. (S) Basic testing functionality required for testing query upgrades 
1. (S) Supporting chained statement execution (i.e. one CSAS as the input to another)
1. (S) Supported meta-directives other than `--@expected.error`
1. (M) Extending ksqlDB to support the language gaps
1. (M) Historical plans
1. (L) Migrate (R)QTT use cases

## Documentation

Documentation for this testing tool will be another entry into the documentation suite we
currently maintain. It will contain an enumeration of the special directives as well as any
unsupported operations.

## Security Implications

N/A