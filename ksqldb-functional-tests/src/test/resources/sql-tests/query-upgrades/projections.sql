-- this file tests adding/removing/changing projection columns

----------------------------------------------------------------------------------------------------
--@test: add columns to DDL stream
-- this test uses a DML to test because of https://github.com/confluentinc/ksql/issues/6058
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE OR REPLACE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);


----------------------------------------------------------------------------------------------------
--@test: add columns to DDL table
-- this test uses a DML to test because of https://github.com/confluentinc/ksql/issues/6058
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE OR REPLACE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT * FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to stream
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);

CREATE OR REPLACE STREAM b AS SELECT id, col1, col2 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to stream via some computation
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);

CREATE OR REPLACE STREAM b AS SELECT id, col1, col1 + 1 AS plus_one FROM a;

INSERT INTO a (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, col1, plus_one) VALUES (1, 1, 2);

----------------------------------------------------------------------------------------------------
--@test: add columns to stream via select *
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);

CREATE OR REPLACE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to table
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);

CREATE OR REPLACE TABLE b AS SELECT id, col1, col2 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to stream with PARTITION BY
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);

CREATE OR REPLACE STREAM b AS SELECT id, col1, col2 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);
ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add aggregate columns to value
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, COUNT(*) AS cnt FROM a GROUP BY id;

INSERT INTO a (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, cnt) VALUES (1, 1);

CREATE OR REPLACE TABLE b AS SELECT id, COUNT(*) AS cnt, AS_VALUE(id) AS id_val FROM a GROUP BY id;

INSERT INTO a (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, cnt, id_val) VALUES (1, 2, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to DML stream in middle
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL1` INTEGER, `COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2 FROM a;

CREATE OR REPLACE STREAM b AS SELECT id, col2, col1 FROM a;

----------------------------------------------------------------------------------------------------
--@test: remove column from DDL stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE OR REPLACE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: remove column from DML stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2 FROM a;
CREATE OR REPLACE STREAM b AS SELECT id, col1 FROM a;

----------------------------------------------------------------------------------------------------
--@test: change column type in DML stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2 FROM a;
CREATE OR REPLACE STREAM b AS SELECT id, col1, CAST(col2 AS STRING) AS col2 FROM a;

----------------------------------------------------------------------------------------------------
--@test: change column name in DML stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2 FROM a;
CREATE OR REPLACE STREAM b AS SELECT id, col1, col2 AS col3 FROM a;

----------------------------------------------------------------------------------------------------
--@test: remove column from DML stream with select *
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`PLUS_ONE` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1, col2, col1 + 1 AS plus_one FROM a;
CREATE OR REPLACE STREAM b AS SELECT * FROM a;

----------------------------------------------------------------------------------------------------
--@test: remove column from DDL table
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE OR REPLACE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');

----------------------------------------------------------------------------------------------------
--@test: remove column from DML table
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (The following columns are changed, missing or reordered: [`COL2` INTEGER])
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT id, col1, col2 FROM a;
CREATE OR REPLACE TABLE b AS SELECT id, col1 FROM a;