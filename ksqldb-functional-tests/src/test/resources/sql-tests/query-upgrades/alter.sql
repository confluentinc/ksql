-- this file tests adding columns

----------------------------------------------------------------------------------------------------
--@test: add columns to DDL stream
-- this test uses a DML to test because of https://github.com/confluentinc/ksql/issues/6058
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
ALTER STREAM a ADD COLUMN col2 INT;

CREATE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);


----------------------------------------------------------------------------------------------------
--@test: add columns to DDL table
-- this test uses a DML to test because of https://github.com/confluentinc/ksql/issues/6058
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
ALTER TABLE a ADD COLUMN col2 INT;

CREATE TABLE b AS SELECT * FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

----------------------------------------------------------------------------------------------------
--@test: add columns to stream defined by CSAS
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: ALTER command is not supported for CREATE ... AS statements.
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT id, col1 FROM a;
ALTER STREAM b ADD COLUMN col2 INT;

----------------------------------------------------------------------------------------------------
--@test: add columns to table defined by CTAS
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: ALTER command is not supported for CREATE ... AS statements.
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT id, col1 FROM a;
ALTER TABLE b ADD COLUMN col2 INT;

----------------------------------------------------------------------------------------------------
--@test: add an already existing column to a stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Cannot add column `COL1` to schema. A column with the same name already exists.
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
ALTER STREAM a ADD COLUMN col1 INT;

----------------------------------------------------------------------------------------------------
--@test: alter a nonexistent stream
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Source A does not exist
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

ALTER STREAM a ADD COLUMN col1 INT;