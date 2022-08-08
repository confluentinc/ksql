-- this file tests adding/removing/changing filters

----------------------------------------------------------------------------------------------------
--@test: add filter to basic STREAM without filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1) VALUES (1, 0);
ASSERT VALUES b (id, col1) VALUES (1, 0);

CREATE OR REPLACE STREAM b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (1, 1);

ASSERT VALUES b (id, col1) VALUES (1, 1);

----------------------------------------------------------------------------------------------------
--@test: remove filter from basic STREAM with filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (2, 0);

CREATE OR REPLACE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1) VALUES (1, 0);

ASSERT VALUES b (id, col1) VALUES (1, 0);

----------------------------------------------------------------------------------------------------
--@test: modify filter from basic STREAM with filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (1, 1);

CREATE OR REPLACE STREAM b AS SELECT * FROM a WHERE col1 < 0;

INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (1, -1);

ASSERT VALUES b (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, col1) VALUES (1, -1);

----------------------------------------------------------------------------------------------------
--@test: add filter to basic TABLE without filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT * FROM a;

INSERT INTO a (id, col1) VALUES (1, 0);

ASSERT VALUES b (id, col1) VALUES (1, 0);

CREATE OR REPLACE TABLE b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 0);
-- note, though the table contains the row [1->0], and this update does not pass the filter,
-- no tombstone is emitted. See https://github.com/confluentinc/ksql/issues/6493.

INSERT INTO a (id, col1) VALUES (1, 1);

ASSERT VALUES b (id, col1) VALUES (1, 1);

INSERT INTO a (id, col1) VALUES (1, 0);

ASSERT NULL VALUES b (id) KEY (1);

----------------------------------------------------------------------------------------------------
--@test: remove filter from basic TABLE with filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 1);
INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (1, 2);

ASSERT VALUES b (id, col1) VALUES (1, 1);
ASSERT NULL VALUES b (id) KEY (1);
ASSERT VALUES b (id, col1) VALUES (1, 2);

CREATE OR REPLACE TABLE b AS SELECT * FROM a;

INSERT INTO a (id, col1) VALUES (1, -1);

ASSERT VALUES b (id, col1) VALUES (1, -1);

----------------------------------------------------------------------------------------------------
--@test: modify filter from basic TABLE with filter
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT * FROM a WHERE col1 > 0;

INSERT INTO a (id, col1) VALUES (1, 1);
INSERT INTO a (id, col1) VALUES (1, 0);
INSERT INTO a (id, col1) VALUES (1, 2);

ASSERT VALUES b (id, col1) VALUES (1, 1);
ASSERT NULL VALUES b (id) KEY (1);
ASSERT VALUES b (id, col1) VALUES (1, 2);

CREATE OR REPLACE TABLE b AS SELECT * FROM a WHERE col1 < 0;

INSERT INTO a (id, col1) VALUES (1, 3);
-- note, though the table contains the row [1->2], and this update does not pass the filter,
-- no tombstone is emitted. See https://github.com/confluentinc/ksql/issues/6493.

INSERT INTO a (id, col1) VALUES (1, -1);
INSERT INTO a (id, col1) VALUES (1, 5);

ASSERT VALUES b (id, col1) VALUES (1, -1);
ASSERT NULL VALUES b (id) KEY (1);

----------------------------------------------------------------------------------------------------
--@test: add filter to StreamTableJoin
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Upgrades not yet supported for StreamTableJoin
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';
CREATE STREAM s (id INT KEY, foo INT) WITH (kafka_topic='s', value_format='JSON');
CREATE TABLE t (id INT PRIMARY KEY, bar INT) WITH (kafka_topic='t', value_format='JSON');

CREATE STREAM j AS SELECT s.id, s.foo, t.bar FROM s JOIN t ON s.id = t.id;
CREATE OR REPLACE STREAM j AS SELECT s.id, s.foo, t.bar FROM s JOIN t ON s.id = t.id WHERE s.foo > 0;

----------------------------------------------------------------------------------------------------
--@test: change filter in StreamAggregate (StreamGroupByKey)
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(*) as count FROM foo WHERE col1 >= 0 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, 0);

ASSERT VALUES bar (id, count) VALUES (1, 1);
ASSERT VALUES bar (id, count) VALUES (2, 1);

CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(*) as count FROM foo WHERE col1 > 0 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, 1);

ASSERT VALUES bar (id, count) VALUES (2, 2);

----------------------------------------------------------------------------------------------------
--@test: change filter in StreamAggregate (StreamGroupBy)
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT col1, COUNT(*) as count FROM foo WHERE col1 >= 0 GROUP BY col1;

INSERT INTO foo (col1, id) VALUES (1, 0);
INSERT INTO foo (col1, id) VALUES (2, 0);

ASSERT VALUES bar (col1, count) VALUES (1, 1);
ASSERT VALUES bar (col1, count) VALUES (2, 1);

CREATE OR REPLACE TABLE bar AS SELECT col1, COUNT(*) as count FROM foo WHERE col1 > 1 GROUP BY col1;

INSERT INTO foo (col1, id) VALUES (1, 0);
INSERT INTO foo (col1, id) VALUES (2, 1);

ASSERT VALUES bar (col1, count) VALUES (2, 2);

----------------------------------------------------------------------------------------------------
--@test: add filter in StreamAggregate where columns are already in input schema
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, 0);

ASSERT VALUES bar (id, count) VALUES (1, 1);
ASSERT VALUES bar (id, count) VALUES (2, 1);

CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE col1 > 0 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, 1);

ASSERT VALUES bar (id, count) VALUES (2, 2);

----------------------------------------------------------------------------------------------------
--@test: remove filter in StreamAggregate where columns are already in input schema
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE col1 > 0 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 1);

ASSERT VALUES bar (id, count) VALUES (1, 1);

CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);

ASSERT VALUES bar (id, count) VALUES (1, 2);

----------------------------------------------------------------------------------------------------
--@test: change filter in StreamAggregate to another column that already exists in input
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE col1 >= 0 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, 0);

ASSERT VALUES bar (id, count) VALUES (1, 1);
ASSERT VALUES bar (id, count) VALUES (2, 1);

CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE id > 1 GROUP BY id;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (2, -1);

ASSERT VALUES bar (id, count) VALUES (2, 2);

----------------------------------------------------------------------------------------------------
--@test: change filter in materialized table
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE col1 >= 0 GROUP BY id;
CREATE TABLE baz AS SELECT * FROM bar;

INSERT INTO foo (id, col1) VALUES (1, 0);

ASSERT VALUES baz (id, count) VALUES (1, 1);

CREATE OR REPLACE TABLE baz AS SELECT * FROM bar WHERE count > 2;

INSERT INTO foo (id, col1) VALUES (1, 0);
INSERT INTO foo (id, col1) VALUES (1, 0);

ASSERT VALUES baz (id, count) VALUES (1, 3);

----------------------------------------------------------------------------------------------------
--@test: remove filter in StreamAggregate where columns are not already in input schema
--@test: add filter in StreamAggregate where columns are not in input schema
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: StreamAggregate must have matching columns not part of aggregate. Values differ: [`ID`, `ROWTIME`, `COL1`] vs. [`ID`, `ROWTIME`]
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(*) as count FROM foo WHERE col1 > 0 GROUP BY id;

CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(*) as count FROM foo GROUP BY id;

----------------------------------------------------------------------------------------------------
-- until we think this through a little bit more, don't allow changing non-aggregate columns
-- to StreamAggregate nodes, though this should technically be OK

--@test: add filter in StreamAggregate where columns are not in input schema
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: StreamAggregate must have matching columns not part of aggregate. Values differ: [`ID`, `ROWTIME`] vs. [`ID`, `COL1`]
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM foo (id INT KEY, col1 INT) WITH (kafka_topic='foo', value_format='JSON');
CREATE TABLE bar AS SELECT id, COUNT(*) as count FROM foo GROUP BY id;
CREATE OR REPLACE TABLE bar AS SELECT id, COUNT(col1) as count FROM foo WHERE col1 > 0 GROUP BY id;

----------------------------------------------------------------------------------------------------
--@test: add filter to PartitionBy
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a PARTITION BY col1;

INSERT INTO a (id, col1) VALUES (0, 0);
ASSERT VALUES b (id, col1) VALUES (0, 0);

CREATE OR REPLACE STREAM b AS SELECT * FROM a WHERE col1 > 0 PARTITION BY col1;

INSERT INTO a (id, col1) VALUES (0, 0);
INSERT INTO a (id, col1) VALUES (0, 1);

ASSERT VALUES b (id, col1) VALUES (0, 1);

----------------------------------------------------------------------------------------------------
--@test: add filter to windowed aggregation
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Upgrades not yet supported for StreamWindowedAggregate
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT id, COUNT(*)
  FROM a WINDOW TUMBLING (SIZE 30 SECONDS)
  GROUP BY id;

CREATE OR REPLACE TABLE b AS SELECT id, COUNT(*)
  FROM a WINDOW TUMBLING (SIZE 30 SECONDS)
  WHERE col1 > 0
  GROUP BY id;