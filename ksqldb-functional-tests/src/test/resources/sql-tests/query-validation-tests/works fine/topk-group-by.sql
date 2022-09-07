--@test: topk-group-by - topk integer - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic',value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 100, 99]);

--@test: topk-group-by - topk integer - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic',value_format='JSON');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 100, 99]);

--@test: topk-group-by - topk integer - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic',value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 100, 99]);

--@test: topk-group-by - topk integer - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE integer) WITH (kafka_topic='test_topic',value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 0);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 0]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 99, 7]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[100, 100, 99]);

--@test: topk-group-by - topk long - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 100]);

--@test: topk-group-by - topk long - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 100]);

--@test: topk-group-by - topk long - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 100]);

--@test: topk-group-by - topk long - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE bigint) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 99]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648, 100, 100]);

--@test: topk-group-by - topk double - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7.3);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 100.5]);

--@test: topk-group-by - topk double - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7.3);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 100.5]);

--@test: topk-group-by - topk double - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7.3);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 100.5]);

--@test: topk-group-by - topk double - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE double) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 2147483648.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 99.9);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 7.3);
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 100.5);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 99.9]);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY[2147483648.9, 100.5, 100.5]);

--@test: topk-group-by - topk string - AVRO
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE string) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'a');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'c');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'd');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'b']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['d', 'c', 'b']);

--@test: topk-group-by - topk string - JSON
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'a');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'c');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'd');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'b']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['d', 'c', 'b']);

--@test: topk-group-by - topk string - PROTOBUF
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE string) WITH (kafka_topic='test_topic', value_format='PROTOBUF');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'a');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'c');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'd');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'b']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['d', 'c', 'b']);

--@test: topk-group-by - topk string - PROTOBUF_NOSR
CREATE STREAM TEST (ID BIGINT KEY, NAME varchar, VALUE string) WITH (kafka_topic='test_topic', value_format='PROTOBUF_NOSR');
CREATE TABLE S2 as SELECT ID, topk(value, 3) as topk FROM test group by id;
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'a');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'c');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'b');
INSERT INTO `TEST` (ID, NAME, value) VALUES (0, 'zero', 'd');
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'a']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['c', 'b', 'b']);
ASSERT VALUES `S2` (ID, TOPK) VALUES (0, ARRAY['d', 'c', 'b']);

