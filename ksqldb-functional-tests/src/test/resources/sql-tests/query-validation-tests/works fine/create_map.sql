--@test: create_map - create map from named tuples
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(k1:=v1, k2:=v1*2) as M FROM TEST;
INSERT INTO `TEST` (k1, k2, v1) VALUES ('foo', 'bar', 10);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('foo':=10, 'bar':=20));

--@test: create_map - create map from key/value lists
CREATE STREAM TEST (ID STRING KEY, ks ARRAY<VARCHAR>, vals ARRAY<INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, AS_MAP(ks, vals) as m FROM TEST;
INSERT INTO `TEST` (ks, vals) VALUES (ARRAY['a', 'b'], ARRAY[1, 2]);
INSERT INTO `TEST` (ks, vals) VALUES (ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]);
INSERT INTO `TEST` (ks, vals) VALUES (ARRAY['a', 'b'], ARRAY[1, 2, 3]);
INSERT INTO `TEST` (ks, vals) VALUES (ARRAY['a', 'b', 'c'], ARRAY[1, 2, NULL]);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=1, 'b':=2));
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=1, 'b':=2, 'c':=3));
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=1, 'b':=2));
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=1, 'b':=2, 'c':=NULL));

--@test: create_map - create map from named tuples compatible mismatching types
CREATE STREAM TEST (k1 VARCHAR, k2 VARCHAR, v1 BOOLEAN) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT MAP(k1:=v1, k2:='true', 10:='no') as M FROM TEST;
INSERT INTO `TEST` (k1, k2, v1) VALUES ('a', 'b', true);
INSERT INTO `TEST` (k1, k2, v1) VALUES ('a', 'b', false);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=true, 'b':=true, '10':=false));
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('a':=false, 'b':=true, '10':=false));

--@test: create_map - create map from named tuples incompatible mismatching types
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: invalid input syntax for type INTEGER: "hello".
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(k1:=v1, k2:='hello') as M FROM TEST;
--@test: create_map - create map from named tuples all null values
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot construct a map with all NULL values
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(k1:=NULL, k2:=NULL) as M FROM TEST;
--@test: create_map - create map from named tuples and some values
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(k1:=v1, k2:=NULL) as M FROM TEST;
INSERT INTO `TEST` (k1, k2, v1) VALUES ('foo', 'bar', 10);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('foo':=10, 'bar':=NULL));

--@test: create_map - create map from named tuples and all null key
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Cannot construct a map with all NULL keys
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(null:=v1) as M FROM TEST;
--@test: create_map - create map from named tuples and some null key
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(k1:=v1, NULL:=v1) as M FROM TEST;
INSERT INTO `TEST` (k1, k2, v1) VALUES ('foo', 'bar', 10);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('foo':=10, 'null':=10));

--@test: create_map - create map from named tuples and cast null key
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP(CAST(NULL AS STRING) := v1) as M FROM TEST;
INSERT INTO `TEST` (k1, k2, v1) VALUES ('foo', 'bar', 10);
ASSERT VALUES `OUTPUT` (M) VALUES (MAP('null':=10));

--@test: create_map - create empty map
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Map constructor cannot be empty. Please supply at least one key value pair (see https://github.com/confluentinc/ksql/issues/4239).
CREATE STREAM TEST (ID STRING KEY, k1 VARCHAR, k2 VARCHAR, v1 INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, MAP() as M FROM TEST;
