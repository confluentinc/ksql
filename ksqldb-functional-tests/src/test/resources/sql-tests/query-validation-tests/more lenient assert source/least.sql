--@test: least - test least with integer
CREATE STREAM INPUT (ID BIGINT KEY, I1 INT, I2 INT, I3 INT) WITH (kafka_topic='input_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(I1, I2, I3, null, 5) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, I1, I2, I3) VALUES (1, 2147483647, -2147483648, NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, -2147483648);
ASSERT stream OUTPUT (ID BIGINT KEY, LOWEST INT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: least - test least with bigint
CREATE STREAM INPUT (ID BIGINT KEY, B1 BIGINT, B2 BIGINT, B3 BIGINT) WITH (kafka_topic='input_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(B1, B2, B3, null) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, B1, B2, B3) VALUES (1, 9223372036854775807, -9223372036854775808, NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, -9223372036854775808);
ASSERT stream OUTPUT (ID BIGINT KEY, LOWEST BIGINT) WITH (KAFKA_TOPIC='OUTPUT');

--@test: least - test least with decimal
CREATE STREAM INPUT (ID BIGINT KEY, DE1 DECIMAL(9,3), DE2 DECIMAL(9,3), DE3 DECIMAL(9,3)) WITH (kafka_topic='input_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(DE1, DE2, DE3, null) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, DE1, DE2, DE3) VALUES (1, 123456.789, -987654.321, NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, -987654.321);
ASSERT stream OUTPUT (ID BIGINT KEY, LOWEST DECIMAL(9,3)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: least - test least with double
CREATE STREAM INPUT (ID BIGINT KEY, DO1 DOUBLE, DO2 DOUBLE, DO3 DOUBLE) WITH (kafka_topic='input_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(DO1, DO2, DO3, null) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, DO1, DO2, DO3) VALUES (1, 50000.555, -99999.555, NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, -99999.555);
ASSERT stream OUTPUT (ID BIGINT KEY, LOWEST DOUBLE) WITH (KAFKA_TOPIC='OUTPUT');

--@test: least - test implicit casting to long
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' cannot be resolved due to ambiguous method parameters (INTEGER, BIGINT).
CREATE STREAM INPUT (ID BIGINT KEY, N1 INT, N2 BIGINT) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(N1, N2) AS LOWEST FROM INPUT;
--@test: least - test implicit casting to decimal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' cannot be resolved due to ambiguous method parameters (INTEGER, BIGINT, DECIMAL(50, 30)).
CREATE STREAM INPUT (ID BIGINT KEY, N1 INT, N2 BIGINT, N3 DECIMAL(50,30)) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(N1, N2, N3) AS LOWEST FROM INPUT;
--@test: least - test implicit casting to double
CREATE STREAM INPUT (ID BIGINT KEY, N1 INT, N2 BIGINT, N3 DECIMAL(30,18), N4 DOUBLE) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(N1, N2, N3, N4, null) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, N1, N2, N3) VALUES (1, 1, -1000000000000, -99999.99999);
INSERT INTO `INPUT` (ID, N1, N2, N3) VALUES (2, 1, -1, -55555.555555555555555555);
INSERT INTO `INPUT` (ID, N1, N2, N3) VALUES (3, -500000, -1, -99999.99999);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, -1000000000000);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, -55555.555555555555);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, -500000.0);

--@test: least - test least with strings
CREATE STREAM INPUT (ID BIGINT KEY, S1 STRING, S2 STRING, S3 STRING, S4 STRING) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(S1, S2, S3, S4, null, null, 'hello') AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (1, 'apple', 'banana', 'aardvark', NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (2, NULL, NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (3, '!', 'Zebra', 'aardvark', NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, 'aardvark');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, 'hello');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, '!');

--@test: least - test decimal widening
CREATE STREAM INPUT (ID BIGINT KEY, N1 DECIMAL(9,2), N2 DECIMAL (7,6)) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(N1, N2) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, N1, N2) VALUES (1, 1234567.89, 0.123456);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, 0.123456);
ASSERT stream OUTPUT (ID BIGINT KEY, LOWEST DECIMAL(13,6)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: least - test all null input
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' cannot be resolved due to ambiguous method parameters (null, null, null, null).
CREATE STREAM INPUT (ID BIGINT KEY) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, least(null, null, null, null) AS LOWEST FROM INPUT;
--@test: least - test no parameters
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' does not accept parameters ().
CREATE STREAM INPUT (ID STRING KEY) WITH (kafka_topic='test_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST() AS LOWEST FROM INPUT;
--@test: least - test trying to compare numeric and string
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' does not accept parameters (STRING, INTEGER).
CREATE STREAM INPUT (ID STRING KEY, NUM INT) WITH (kafka_topic='test_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(ID, NUM) AS LOWEST FROM INPUT;
--@test: least - test trying to call LEAST(*)
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Function 'least' does not accept parameters ().
CREATE STREAM INPUT (ID STRING KEY, NUM INT) WITH (kafka_topic='test_topic', value_format='json');
CREATE STREAM OUTPUT AS SELECT LEAST(*) AS LOWEST FROM INPUT;
--@test: least - test least with bytes
CREATE STREAM INPUT (ID BIGINT KEY, S1 BYTES, S2 BYTES, S3 BYTES, S4 BYTES) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(S1, S2, S3, S4, null, null, TO_BYTES('hello', 'ascii')) AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (1, 'YXBwbGU=', 'YmFuYW5h', 'YWFyZGF2YXJr', NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (2, NULL, NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (3, 'IQ==', 'emVicmE=', 'YWFyZGF2YXJr', NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, 'YWFyZGF2YXJr');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, 'aGVsbG8=');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, 'IQ==');

--@test: least - test least with dates
CREATE STREAM INPUT (ID BIGINT KEY, S1 DATE, S2 DATE, S3 DATE, S4 DATE) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(S1, S2, S3, S4, null, null, '2022-06-14') AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (1, '2022-03-20', '2021-07-16', '1953-11-23', NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (2, NULL, NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (3, '2022-06-15', '2022-07-14', '1969-10-01', NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, '1953-11-23');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, '2022-06-14');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, '1969-10-01');

--@test: least - test least with times
CREATE STREAM INPUT (ID BIGINT KEY, S1 TIME, S2 TIME, S3 TIME, S4 TIME) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(S1, S2, S3, S4, null, null, '09:16:00') AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (1, '09:15:59', '06:29:21', '00:00', NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (2, NULL, NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (3, '09:02:40', '09:16:10', '09:16', NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, '00:00');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, '09:16');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, '09:02:40');

--@test: least - test least with timestamps
CREATE STREAM INPUT (ID BIGINT KEY, S1 TIMESTAMP, S2 TIMESTAMP, S3 TIMESTAMP, S4 TIMESTAMP) WITH (kafka_topic='input_topic',value_format='json');
CREATE STREAM OUTPUT AS SELECT ID, LEAST(S1, S2, S3, S4, null, null, '2022-06-14') AS LOWEST FROM INPUT;
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (1, '2022-03-20T00:00:00.000', '2021-07-16T00:00:00.000', '1953-11-23T00:00:00.000', NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (2, NULL, NULL, NULL, NULL);
INSERT INTO `INPUT` (ID, S1, S2, S3, S4) VALUES (3, '2022-06-15T00:00:00.000', '2022-07-14T00:00:00.000', '1969-10-01T00:00:00.000', NULL);
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (1, '1953-11-23T00:00:00.000');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (2, '2022-06-14T00:00:00.000');
ASSERT VALUES `OUTPUT` (ID, LOWEST) VALUES (3, '1969-10-01T00:00:00.000');

