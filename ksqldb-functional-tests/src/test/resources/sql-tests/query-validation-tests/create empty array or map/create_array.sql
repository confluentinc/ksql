--@test: create_array - construct a list from two elements
CREATE STREAM TEST (ID STRING KEY, a INT, b INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[a, b, 3] as l FROM TEST;
INSERT INTO `TEST` (a, b) VALUES (1, 2);
INSERT INTO `TEST` (a, b) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (L) VALUES (ARRAY[1, 2, 3]);
ASSERT VALUES `OUTPUT` (L) VALUES (ARRAY[NULL, NULL, 3]);

--@test: create_array - construct a list from null casted elements
CREATE STREAM TEST (ID STRING KEY, a INT, b INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[CAST(NULL AS INT)] as l FROM TEST;
INSERT INTO `TEST` (a, b) VALUES (1, 2);
ASSERT VALUES `OUTPUT` (L) VALUES (ARRAY[NULL]);

--@test: create_array - construct a list from no elements
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Array constructor cannot be empty. Please supply at least one element (see https://github.com/confluentinc/ksql/issues/4239).
CREATE STREAM TEST (ID STRING KEY, a INT, b INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[] as l FROM TEST;
--@test: create_array - construct a list from null non-casted elements
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Cannot construct an array with all NULL elements (see https://github.com/confluentinc/ksql/issues/4239). As a workaround, you may cast a NULL value to the desired type.
CREATE STREAM TEST (ID STRING KEY, a INT, b INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[NULL] as l FROM TEST;
--@test: create_array - construct a list from compatible mismatching elements
CREATE STREAM TEST (ID STRING KEY, a INT, b BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[null, 10, '2e1', a, b] as l FROM TEST;
INSERT INTO `TEST` (a, b) VALUES (30, 40);
ASSERT VALUES `OUTPUT` (L) VALUES (ARRAY[NULL, 10, 20, 30, 40]);
ASSERT stream OUTPUT (ID STRING KEY, L ARRAY<DECIMAL(19, 0)>) WITH (KAFKA_TOPIC='OUTPUT');

--@test: create_array - construct a list from compatible mismatching struct elements
CREATE STREAM TEST (a INT, b BIGINT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ARRAY[null, struct(x := a, y := b), struct(y := a, z:= b)] as l FROM TEST;
INSERT INTO `TEST` (a, b) VALUES (30, 40);
ASSERT VALUES `OUTPUT` (L) VALUES (ARRAY[NULL, STRUCT(X:=30, Y:=40, Z:=NULL), STRUCT(X:=NULL, Y:=30, Z:=40)]);

--@test: create_array - construct a list from incompatible mismatching elements
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: invalid input syntax for type INTEGER: "not a number"
CREATE STREAM TEST (ID STRING KEY, a INT, b INT) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ARRAY[1, 'not a number'] as l FROM TEST;
