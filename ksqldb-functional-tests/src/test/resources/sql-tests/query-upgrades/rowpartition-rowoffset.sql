----------------------------------------------------------------------------------------------------
--@test: should fail on CREATE OR REPLACE of a legacy query with ROWPARTITION
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Select: Cannot find the select field in the available fields. field: `ROWPARTITION`
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE TABLE b AS SELECT id, col1, ROWPARTITION AS ro FROM a WHERE col1 > 0 EMIT CHANGES;

----------------------------------------------------------------------------------------------------
--@test: should fail on CREATE OR REPLACE of a legacy query with ROWOFFSET
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: Cannot find the select field in the available fields. field: `ROWOFFSET`
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE TABLE b AS SELECT id, col1 FROM a WHERE col1 > ROWOFFSET EMIT CHANGES;

----------------------------------------------------------------------------------------------------
--@test: should fail to query existing streams with user column named the same as a pseudocolumn - different type
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Your stream/table has columns with the same name as newly introduced pseudocolumns in ksqlDB, and cannot be queried as a result.
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE STREAM a (id INT KEY, ROWOFFSET STRING) WITH (kafka_topic='a', value_format='JSON');

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE STREAM b AS SELECT ROWOFFSET AS ro FROM a;

----------------------------------------------------------------------------------------------------
--@test: should fail to query existing streams with user column named the same as a pseudocolumn - same type
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Your stream/table has columns with the same name as newly introduced pseudocolumns in ksqlDB, and cannot be queried as a result.
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE STREAM a (id INT KEY, ROWPARTITION BIGINT) WITH (kafka_topic='a', value_format='JSON');

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE STREAM b AS SELECT ROWOFFSET AS ro FROM a;