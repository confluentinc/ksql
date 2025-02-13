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
