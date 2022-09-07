--@test: multi-col-keys - select * from stream
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2, V) VALUES (NULL, 2, 0);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (NULL, 2, 0);
ASSERT VALUES `OUTPUT` (V) VALUES (0);
ASSERT stream OUTPUT (K INT KEY, K2 INT KEY, V INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select * from table
CREATE TABLE INPUT (K INT PRIMARY KEY, K2 INT PRIMARY KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2, V) VALUES (NULL, 2, 0);
INSERT INTO `INPUT` (K, K2) VALUES (NULL, 2);
INSERT INTO `INPUT` (V) VALUES (0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (NULL, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2) VALUES (NULL, 2);
ASSERT table OUTPUT (K INT PRIMARY KEY, K2 INT PRIMARY KEY, V INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select * with WHERE on single key
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE K = 1;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2, V) VALUES (2, 1, 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT stream OUTPUT (K INT KEY, K2 INT KEY, V INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select * with WHERE on both key
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE K = 1 AND K2 = 2;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 1, 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT stream OUTPUT (K INT KEY, K2 INT KEY, V INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select * from stream partition by single key col
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY K;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT stream OUTPUT (K INT KEY, V INT, K2 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select * from stream partition by struct representing the key
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT PARTITION BY STRUCT(K:=K, K2:=K2);
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (KSQL_COL_0, V, K, K2) VALUES (STRUCT(K:=1, K2:=2), 0, 1, 2);
ASSERT stream OUTPUT (KSQL_COL_0 STRUCT<K INT, K2 INT> KEY, V INT, K INT, K2 INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - group by one col
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) as COUNT FROM INPUT GROUP BY K;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, COUNT) VALUES (1, 1);
ASSERT table OUTPUT (K INTEGER PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - group by two cols
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, K2, COUNT(*) as COUNT FROM INPUT GROUP BY K, K2;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2, COUNT) VALUES (1, 2, 1);
ASSERT table OUTPUT (K INTEGER PRIMARY KEY, K2 INTEGER PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - group by one col table with tombstones
CREATE TABLE INPUT (K INT PRIMARY KEY, K2 INT PRIMARY KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) as COUNT FROM INPUT GROUP BY K;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, COUNT) VALUES (1, 1);
ASSERT VALUES `OUTPUT` (K, COUNT) VALUES (1, 0);

--@test: multi-col-keys - group by two cols table with tombstones
CREATE TABLE INPUT (K INT PRIMARY KEY, K2 INT PRIMARY KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, K2, COUNT(*) as COUNT FROM INPUT GROUP BY K, K2;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `INPUT` (K, K2) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (K, K2, COUNT) VALUES (1, 2, 1);
ASSERT VALUES `OUTPUT` (K, K2, COUNT) VALUES (1, 2, 0);

--@test: multi-col-keys - group by two cols with AS_VALUE copies
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, K2, AS_VALUE(K) as kv, AS_VALUE(K2) as kv2, COUNT(*) as COUNT FROM INPUT GROUP BY K, K2;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (K, K2, KV, KV2, COUNT) VALUES (1, 2, 1, 2, 1);
ASSERT table OUTPUT (K INTEGER PRIMARY KEY, K2 INTEGER PRIMARY KEY, KV INT, KV2 INT, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - windowed group by one col
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE TABLE OUTPUT AS SELECT K, COUNT(*) as COUNT FROM INPUT WINDOW TUMBLING (SIZE 1 SECOND) GROUP BY K;
INSERT INTO `INPUT` (K, K2, V, ROWTIME) VALUES (1, 2, 0, 0);
INSERT INTO `INPUT` (K, K2, V, ROWTIME) VALUES (1, 2, 0, 0);
INSERT INTO `INPUT` (K, K2, V, ROWTIME) VALUES (1, 2, 0, 1001);
ASSERT VALUES `OUTPUT` (K, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 1, 0, 1000);
ASSERT VALUES `OUTPUT` (K, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 2, 0, 1000);
ASSERT VALUES `OUTPUT` (K, COUNT, WINDOWSTART, WINDOWEND) VALUES (1, 1, 1000, 2000);
ASSERT table OUTPUT (K INTEGER PRIMARY KEY, COUNT BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - join with repartition on single key
CREATE STREAM S1 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s1', format='JSON');
CREATE STREAM S2 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s2', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM S1 JOIN S2 WITHIN 1 DAY on S1.K = S2.K;
INSERT INTO `S1` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `S2` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (S1_K, S1_K2, S1_V, S2_K, S2_K2, S2_V) VALUES (1, 2, 0, 1, 2, 0);
ASSERT stream OUTPUT (S1_K INTEGER KEY, S1_K2 INTEGER, S1_V INTEGER, S2_K INTEGER, S2_K2 INTEGER, S2_V INTEGER) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - join with repartition on single value
CREATE STREAM S1 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s1', format='JSON');
CREATE STREAM S2 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s2', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM S1 JOIN S2 WITHIN 1 DAY on S1.v = S2.V;
INSERT INTO `S1` (K, K2, V) VALUES (1, 2, 0);
INSERT INTO `S2` (K, K2, V) VALUES (1, 2, 0);
ASSERT VALUES `OUTPUT` (S1_V, S1_K, S1_K2, S2_K, S2_K2, S2_V) VALUES (0, 1, 2, 1, 2, 0);
ASSERT stream OUTPUT (S1_V INTEGER KEY, S1_K INTEGER, S1_K2 INTEGER, S2_K INTEGER, S2_K2 INTEGER, S2_V INTEGER) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - struct as column in multi-column key
CREATE STREAM INPUT (K INT KEY, K2 STRUCT<F1 INT> KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT;
INSERT INTO `INPUT` (K, K2, V) VALUES (1, STRUCT(F1:=2), 0);
ASSERT VALUES `OUTPUT` (K, K2, V) VALUES (1, STRUCT(F1:=2), 0);
ASSERT stream OUTPUT (K INT KEY, K2 STRUCT<F1 INT> KEY, V INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: multi-col-keys - select only single key col
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: The query used to build `OUTPUT` must include the key columns K and K2 in its projection (eg, SELECT K, K2...).
CREATE STREAM INPUT (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT K, V FROM INPUT;
--@test: multi-col-keys - join on multi-column key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: joins on multiple conditions are not yet supported. Got ((S1.K = S2.K) AND (S1.K2 = S2.K2)).
CREATE STREAM S1 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s1', format='JSON');
CREATE STREAM S2 (K INT KEY, K2 INT KEY, V INT) WITH (kafka_topic='s2', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM S1 JOIN S2 WITHIN 1 DAY on S1.K = S2.K AND S1.K2 = S2.K2;
--@test: multi-col-keys - join to multi-column table
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid join condition: stream-table joins require to join on the table's primary key. Got S.K = T.K.
CREATE STREAM S (K STRING KEY, ID VARCHAR) WITH (kafka_topic='S', format='JSON');
CREATE TABLE NO_KEY (K STRING PRIMARY KEY, K2 STRING PRIMARY KEY, NAME string) WITH (kafka_topic='NO_KEY', format='JSON');
CREATE STREAM OUTPUT as SELECT s.k, name FROM S JOIN NO_KEY t ON s.k = t.k;
