----------------------------------------------------------------------------------------------------
--@test: PARTITION BY - change key column
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (Key columns must be identical.)
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a PARTITION BY col1;

CREATE OR REPLACE STREAM b AS SELECT * FROM a PARTITION BY col2;

----------------------------------------------------------------------------------------------------
--@test: PARTITION BY - add one where didn't previously exist
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (Key columns must be identical.)
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a;

CREATE OR REPLACE STREAM b AS SELECT * FROM a PARTITION BY col1;

----------------------------------------------------------------------------------------------------
--@test: GROUP BY - change grouping columns
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: (Key columns must be identical.)
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT col1, COUNT(*) FROM a GROUP BY col1;

CREATE OR REPLACE TABLE b AS SELECT col2, COUNT(*) FROM a GROUP BY col2;

----------------------------------------------------------------------------------------------------
--@test: GROUP BY - change grouping column ordering
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: StreamGroupBy must have matching group by clause. Values differ: [COL1, COL2] vs. [COL2, COL1]
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE TABLE b AS SELECT col1, col2, COUNT(*) FROM a GROUP BY col1, col2;

CREATE OR REPLACE TABLE b AS SELECT col1, col2, COUNT(*) FROM a GROUP BY col2, col1;
