----------------------------------------------------------------------------------------------------
--@test: upgrade stream across pseudocolumn versions
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE STREAM b AS SELECT id, col1, col2 FROM a WHERE col1 > 0 EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

--partition and offset don't work with SQL testing tool at the moment,
--this just exists as a sanity check to ensure we have enabled the feature.
CREATE STREAM c AS SELECT ID, ROWPARTITION AS rp, ROWOFFSET AS ro FROM b EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES c (id, rp, ro) VALUES (1, 0, 0);

----------------------------------------------------------------------------------------------------
--@test: upgrade table across pseudocolumn versions
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE TABLE b AS SELECT id, col1, col2 FROM a WHERE col1 > 0 EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);

--partition and offset don't work with SQL testing tool at the moment,
--this just exists as a sanity check to ensure we have enabled the feature.
CREATE TABLE c AS SELECT ID, ROWPARTITION AS rp, ROWOFFSET AS ro FROM b EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES c (id, rp, ro) VALUES (1, 0, 0);

----------------------------------------------------------------------------------------------------
--@test: should fail on CREATE OR REPLACE of an old query with ROWPARTITION in SELECT clause
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
--@test: should fail on CREATE OR REPLACE of an old query with ROWOFFSET in SELECT clause
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot find the select field in the available fields. field: `ROWOFFSET`
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE TABLE b AS SELECT id, col1, ROWOFFSET AS ro FROM a WHERE col1 > 0 EMIT CHANGES;

----------------------------------------------------------------------------------------------------
--@test: should fail on CREATE OR REPLACE of a legacy query with ROWPARTITION in WHERE clause
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: Cannot find the select field in the available fields. field: `ROWPARTITION`
----------------------------------------------------------------------------------------------------
SET 'ksql.rowpartition.rowoffset.enabled' = 'false';

CREATE TABLE a (id INT PRIMARY KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE TABLE b AS SELECT id, col1 FROM a EMIT CHANGES;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

SET 'ksql.rowpartition.rowoffset.enabled' = 'true';

CREATE OR REPLACE TABLE b AS SELECT id, col1 FROM a WHERE col1 > ROWPARTITION EMIT CHANGES;

----------------------------------------------------------------------------------------------------
--@test: should fail on CREATE OR REPLACE of a legacy query with ROWOFFSET in WHERE clause
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