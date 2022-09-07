--@test: literals - BOOLEAN literal
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, TRUE, True, true, FALSE, False, false FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5) VALUES (0, true, true, true, false, false, false);

--@test: literals - INT literal min/max
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, -00002147483647 AS MIN, 000002147483647 AS MAX FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, MIN, MAX) VALUES (0, -2147483647, 2147483647);

--@test: literals - BIGINT literal min/max
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, -00009223372036854775807 AS MIN, 000009223372036854775807 AS MAX FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, MIN, MAX) VALUES (0, -9223372036854775807, 9223372036854775807);

--@test: literals - DOUBLE literal min/max
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, 04.90E-324 AS MIN_VALUE, -4.9E-324 AS NEG_MIN_VALUE, 2.2250738585072014E-308 AS MIN_NORMAL, -2.2250738585072014E-308 AS NEG_MIN_NORMAL, 1.7976931348623157E308 AS MAX_VALUE, -1.7976931348623157E308 AS NEG_MAX_VALUE FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, MIN_VALUE, NEG_MIN_VALUE, MIN_NORMAL, NEG_MIN_NORMAL, MAX_VALUE, NEG_MAX_VALUE) VALUES (0, 4.9E-324, -4.9E-324, 2.2250738585072014E-308, -2.2250738585072014E-308, 1.7976931348623157E308, -1.7976931348623157E308);

--@test: literals - DECIMAL literal
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, 2.345 FROM INPUT;
INSERT INTO `INPUT` (ID) VALUES (0);
ASSERT VALUES `OUTPUT` (ID, KSQL_COL_0) VALUES (0, 2.345);
ASSERT stream OUTPUT (K STRING KEY, ID BIGINT, KSQL_COL_0 DECIMAL(4,3)) WITH (KAFKA_TOPIC='OUTPUT');

--@test: literals - BIGINT literal positive overflow
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: Failed to prepare statement: line 2:40: Invalid numeric literal: 9223372036854775808
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, 9223372036854775808 FROM INPUT;
--@test: literals - BIGINT literal negative overflow
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: Invalid numeric literal: -9223372036854775809
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, -9223372036854775809 FROM INPUT;
--@test: literals - DOUBLE literal positive overflow
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: Failed to prepare statement: line 2:40: Number overflows DOUBLE: 1.7976931348623159E308
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, 1.7976931348623159E308 FROM INPUT;
--@test: literals - DOUBLE literal negative overflow
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: Number overflows DOUBLE: -1.7976931348623160E308
CREATE STREAM INPUT (K STRING KEY, ID bigint) WITH (kafka_topic='input', value_format='DELIMITED');
CREATE STREAM OUTPUT AS select K, id, -1.7976931348623160E308 FROM INPUT;
