----------------------------------------------------------------------------------------------------
--@test: add columns and filter to stream
----------------------------------------------------------------------------------------------------
SET 'ksql.create.or.replace.enabled' = 'true';

CREATE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

CREATE STREAM b AS SELECT id, col1 FROM a;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
ASSERT VALUES b (id, col1) VALUES (1, 0);

CREATE OR REPLACE STREAM b AS SELECT id, col1, col2 FROM a WHERE col1 > 0;

INSERT INTO a (id, col1, col2) VALUES (1, 0, 1);
INSERT INTO a (id, col1, col2) VALUES (1, 1, 1);

ASSERT VALUES b (id, col1, col2) VALUES (1, 1, 1);