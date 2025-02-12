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

----------------------------------------------------------------------------------------------------
--@test: ddl evolution
----------------------------------------------------------------------------------------------------

CREATE STREAM a (id INT KEY, col1 INT) WITH (kafka_topic='a', value_format='JSON');
CREATE STREAM b AS SELECT * FROM a;

INSERT INTO a (id, col1) VALUES (1, 1);
ASSERT VALUES b (id, col1) VALUES (1, 1);
CREATE OR REPLACE STREAM a (id INT KEY, col1 INT, col2 INT) WITH (kafka_topic='a', value_format='JSON');

INSERT INTO a (id, col1, col2) VALUES (1, 55, 1);
ASSERT VALUES b (id, col1) VALUES (1, 55);

CREATE STREAM c AS SELECT * FROM b;
CREATE OR REPLACE STREAM b AS SELECT id, col1, col1 + 1 as col1_add FROM a;
INSERT INTO a (id, col1, col2) VALUES (1, 20, 3);
ASSERT VALUES b (id, col1, col1_add) VALUES (1, 20, 21);
ASSERT VALUES c (id, col1) VALUES (1, 20);