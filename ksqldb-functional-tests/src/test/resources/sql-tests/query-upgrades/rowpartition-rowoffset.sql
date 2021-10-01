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