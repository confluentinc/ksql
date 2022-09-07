--@test: join-with-custom-timestamp - stream stream inner join with ts - AVRO
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='AVRO');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='AVRO');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts - JSON
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='JSON');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='JSON');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts - PROTOBUF
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='PROTOBUF');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts - PROTOBUF_NOSR
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF_NOSR');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='PROTOBUF_NOSR');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts extractor both sides - AVRO
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='AVRO');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='AVRO');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts extractor both sides - JSON
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='JSON');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='JSON');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts extractor both sides - PROTOBUF
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='PROTOBUF');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream stream inner join with ts extractor both sides - PROTOBUF_NOSR
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF_NOSR');
CREATE STREAM S2 (ID BIGINT KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='PROTOBUF_NOSR');
CREATE STREAM S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 WITHIN 11 SECONDS ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream table join with ts extractor both sides - AVRO
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='AVRO');
CREATE TABLE  T1 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='t1', value_format='AVRO');
CREATE STREAM S1_JOIN_T1 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, T1.f1, T1.f2 from S1 inner join T1 ON s1.id = t1.id;
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 10000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 10000);
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 90000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 800000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream table join with ts extractor both sides - JSON
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='JSON');
CREATE TABLE  T1 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='t1', value_format='JSON');
CREATE STREAM S1_JOIN_T1 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, T1.f1, T1.f2 from S1 inner join T1 ON s1.id = t1.id;
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 10000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 10000);
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 90000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 800000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream table join with ts extractor both sides - PROTOBUF
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF');
CREATE TABLE  T1 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='t1', value_format='PROTOBUF');
CREATE STREAM S1_JOIN_T1 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, T1.f1, T1.f2 from S1 inner join T1 ON s1.id = t1.id;
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 10000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 10000);
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 90000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 800000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - stream table join with ts extractor both sides - PROTOBUF_NOSR
CREATE STREAM S1 (ID BIGINT KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF_NOSR');
CREATE TABLE  T1 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='t1', value_format='PROTOBUF_NOSR');
CREATE STREAM S1_JOIN_T1 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, T1.f1, T1.f2 from S1 inner join T1 ON s1.id = t1.id;
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 10000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 10000);
INSERT INTO `T1` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 90000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 800000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_T1` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - table table inner join with ts - AVRO
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='AVRO');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='AVRO');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 19000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 18000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 19000, 'foo', 'bar', 19000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 18000, 'blah', 'foo', 18000);

--@test: join-with-custom-timestamp - table table inner join with ts - JSON
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='JSON');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='JSON');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 19000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 18000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 19000, 'foo', 'bar', 19000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 18000, 'blah', 'foo', 18000);

--@test: join-with-custom-timestamp - table table inner join with ts - PROTOBUF
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar) WITH (kafka_topic='s2', value_format='PROTOBUF');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (0, 'blah', 'foo', 10000);
INSERT INTO `S2` (ID, F1, F2, ROWTIME) VALUES (10, 'foo', 'bar', 13000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 19000, 22000);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 18000, 33000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 19000, 'foo', 'bar', 19000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 18000, 'blah', 'foo', 18000);

--@test: join-with-custom-timestamp - table table inner join with ts extractor both sides - AVRO
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='AVRO');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='AVRO');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - table table inner join with ts extractor both sides - JSON
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='JSON');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='JSON');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - table table inner join with ts extractor both sides - PROTOBUF
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='PROTOBUF');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

--@test: join-with-custom-timestamp - table table inner join with ts extractor both sides - PROTOBUF_NOSR
CREATE TABLE S1 (ID BIGINT PRIMARY KEY, NAME varchar, TS bigint) WITH (timestamp='TS', kafka_topic='s1', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 (ID BIGINT PRIMARY KEY, F1 varchar, F2 varchar, RTS bigint) WITH (timestamp='RTS', kafka_topic='s2', value_format='PROTOBUF_NOSR');
CREATE TABLE S1_JOIN_S2 WITH(timestamp='TS') as SELECT S1.ID, S1.name as name, S1.ts as ts, s2.f1, s2.f2 from S1 join S2 ON s1.id = s2.id;
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'zero', 0, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (0, 'blah', 'foo', 10000, 0);
INSERT INTO `S2` (ID, F1, F2, RTS, ROWTIME) VALUES (10, 'foo', 'bar', 13000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (10, '100', 11000, 0);
INSERT INTO `S1` (ID, NAME, TS, ROWTIME) VALUES (0, 'jan', 8000, 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'zero', 0, 'blah', 'foo', 0);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (10, '100', 11000, 'foo', 'bar', 11000);
ASSERT VALUES `S1_JOIN_S2` (S1_ID, NAME, TS, F1, F2, ROWTIME) VALUES (0, 'jan', 8000, 'blah', 'foo', 8000);

