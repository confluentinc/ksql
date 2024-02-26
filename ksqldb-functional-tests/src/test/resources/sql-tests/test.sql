-- this test file is a "meta-test" that tests the basic functionality of the KsqlTester
-- eventually, we plan to remove this test in favor of migrating the QTTs over to use this format
-- directly

----------------------------------------------------------------------------------------------------
--@test: basic test
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', value_format='JSON');

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic test with tables
----------------------------------------------------------------------------------------------------
CREATE TABLE foo (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT * FROM foo;

ASSERT TABLE bar (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='BAR', value_format='JSON');

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic test without rowtime comparison
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (id, col1) VALUES (1, 1);
ASSERT VALUES bar (id, col1) VALUES (1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic test with aggregation
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(*) as count FROM foo GROUP BY id;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, count) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic test (format AVRO)
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='AVRO');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic chained test
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;
CREATE STREAM baz AS SELECT * FROM bar;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES baz (rowtime, id, col1) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: basic join test
----------------------------------------------------------------------------------------------------
CREATE STREAM s (id INT KEY, foo INT) WITH (kafka_topic='s', value_format='JSON');
CREATE TABLE t (id INT PRIMARY KEY, bar INT) WITH (kafka_topic='t', value_format='JSON');

CREATE STREAM j AS SELECT s.id, s.foo, t.bar FROM s JOIN t ON s.id = t.id;

INSERT INTO t (rowtime, id, bar) VALUES (1, 1, 1);
INSERT INTO s (rowtime, id, foo) VALUES (1, 1, 2);

ASSERT VALUES j (rowtime, s_id, foo, bar) VALUES (1, 1, 2, 1);

----------------------------------------------------------------------------------------------------
--@test: basic create or replace test
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 1);

SET 'ksql.create.or.replace.enabled'='true';
CREATE OR REPLACE STREAM bar AS SELECT id, col1 + 1 as col1 FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 2);

----------------------------------------------------------------------------------------------------
--@test: basic test with run script
----------------------------------------------------------------------------------------------------
-- contents of script:
-- CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
-- CREATE STREAM bar AS SELECT * FROM foo;
RUN SCRIPT '/test-script.sql';

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: bad assert statement should fail

--@expected.error: java.lang.AssertionError
--@expected.message: Expected record does not match actual
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES bar (rowtime, id, col1) VALUES (2, 2, 2);

----------------------------------------------------------------------------------------------------
--@test: bad engine statement should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Exception while preparing statement: FOO does not exist.
----------------------------------------------------------------------------------------------------
CREATE STREAM bar AS SELECT * FROM foo;

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong schema should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected schema does not match actual
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 VARCHAR) WITH (kafka_topic='BAR', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong schema (key) should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected schema does not match actual
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT, col1 VARCHAR) WITH (kafka_topic='BAR', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong type should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected type does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT TABLE bar (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='BAR', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong topic should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected kafka topic does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAZ', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong key format should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected key format does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', key_format='KAFKA', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', key_format='AVRO', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong value format should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected value format does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', value_format='AVRO');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong format should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected value format does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', format='KAFKA');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', key_format='KAFKA', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: assert stream with explicit format
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', format='KAFKA');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', key_format='KAFKA', value_format='KAFKA');

----------------------------------------------------------------------------------------------------
--@test: assert stream with explicit expected format
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', key_format='KAFKA', value_format='KAFKA');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', format='KAFKA');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong timestamp column

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected timestamp column does not match actual for source BAR.
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 BIGINT, col2 BIGINT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar WITH(timestamp='col1') AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 BIGINT, col2 BIGINT) WITH (kafka_topic='BAR', value_format='JSON', timestamp='col2');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong timestamp format

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected timestamp format does not match actual for source BAR.
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 VARCHAR) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar WITH(timestamp='col1', timestamp_format='yyyy') AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 VARCHAR) WITH (kafka_topic='BAR', value_format='JSON', timestamp='col1', timestamp_format='mm');

----------------------------------------------------------------------------------------------------
--@test: assert stream with wrong topic should fail

--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Expected value serde features does not match actual for source BAR
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

ASSERT STREAM bar (id INT KEY, col1 INT) WITH (kafka_topic='BAR', value_format='JSON', wrap_single_value=false);

----------------------------------------------------------------------------------------------------
--@test: assert contents of a DDL source
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');

INSERT INTO foo (rowtime, id, col1) VALUES (1, 2, 1);
INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 1);
INSERT INTO foo (rowtime, id, col1) VALUES (1, 3, 1);
ASSERT VALUES foo (rowtime, id, col1) VALUES (1, 2, 1);
ASSERT VALUES foo (rowtime, id, col1) VALUES (1, 1, 1);
ASSERT VALUES foo (rowtime, id, col1) VALUES (1, 3, 1);

----------------------------------------------------------------------------------------------------
--@test: assert a subset of columns
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT, col2 STRING) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1, col2) VALUES (1, 2, 3, 'ABC');
ASSERT VALUES bar (col1) VALUES (3);
INSERT INTO foo (rowtime, id, col1, col2) VALUES (1, 2, 3, 'ABC');
ASSERT VALUES bar (col2, col1) VALUES ('ABC', 3);
INSERT INTO foo (rowtime, id, col1, col2) VALUES (1, 2, 3, 'ABC');
ASSERT VALUES bar (col2, rowtime, id) VALUES ('ABC', 1, 2);
INSERT INTO foo (rowtime, id, col1, col2) VALUES (1, 2, 3, 'ABC');
ASSERT VALUES bar (col1, id) VALUES (3, 2);

----------------------------------------------------------------------------------------------------
--@test: assert tombstones
--@expected.error: java.lang.AssertionError
--@expected.message: Expected record does not match actual
----------------------------------------------------------------------------------------------------
CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (2, 2);

ASSERT NULL VALUES b (id) KEY (1);
ASSERT NULL VALUES b (id) KEY (2);

----------------------------------------------------------------------------------------------------
--@test: drop statement throws error
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The following streams and/or tables read from this source
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM foo2 AS SELECT * FROM foo;
DROP STREAM foo;

----------------------------------------------------------------------------------------------------
--@test: drop statement does not throw error
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM foo2 AS SELECT * FROM foo;
DROP STREAM foo2;
DROP STREAM foo;

----------------------------------------------------------------------------------------------------
--@test: insert bytes
----------------------------------------------------------------------------------------------------
CREATE STREAM foo (id INT KEY, col1 BYTES) WITH (kafka_topic='foo', value_format='JSON');
CREATE STREAM bar AS SELECT * FROM foo;

INSERT INTO foo (rowtime, id, col1) VALUES (1, 1, 'IQ==');
ASSERT VALUES bar (rowtime, id, col1) VALUES (1, 1, 'IQ==');
