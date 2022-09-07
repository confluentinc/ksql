--@test: timestampformat - timestamp format
CREATE STREAM TEST (K STRING KEY, ID bigint, event_timestamp VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='event_timestamp', timestamp_format='yyyy-MM-dd''T''HH:mm:ssX');
CREATE STREAM TS AS select K, id, stringtotimestamp(event_timestamp, 'yyyy-MM-dd''T''HH:mm:ssX') as ets from test;
INSERT INTO `TEST` (ID, event_timestamp, ROWTIME) VALUES (1, '2018-05-11T21:58:33Z', 10);
INSERT INTO `TEST` (ID, event_timestamp, ROWTIME) VALUES (1, '2019-05-11T21:58:33Z', 10);
INSERT INTO `TEST` (ID, event_timestamp, ROWTIME) VALUES (1, '2020-05-11T21:58:33Z', 10);
ASSERT VALUES `TS` (ID, ETS, ROWTIME) VALUES (1, 1526075913000, 1526075913000);
ASSERT VALUES `TS` (ID, ETS, ROWTIME) VALUES (1, 1557611913000, 1557611913000);
ASSERT VALUES `TS` (ID, ETS, ROWTIME) VALUES (1, 1589234313000, 1589234313000);

--@test: timestampformat - with single digit ms and numeric tz
CREATE STREAM TEST (K STRING KEY, event_timestamp VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='event_timestamp', timestamp_format='yyyy-MM-dd''T''HH:mm:ss.SX');
CREATE STREAM TS AS select K, stringtotimestamp(event_timestamp, 'yyyy-MM-dd''T''HH:mm:ss.SX') as ets from test;
INSERT INTO `TEST` (event_timestamp, ROWTIME) VALUES ('2019-08-27T13:31:09.2+0000', 10);
ASSERT VALUES `TS` (ETS, ROWTIME) VALUES (1566912669200, 1566912669200);

--@test: timestampformat - timestamp column of TIMESTAMP type
CREATE STREAM INPUT (K STRING KEY, ID bigint, EVENT_TS TIMESTAMP) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='EVENT_TS');
CREATE STREAM OUTPUT AS SELECT K, ID, EVENT_TS FROM INPUT;
INSERT INTO `INPUT` (ID, EVENT_TS) VALUES (1, '2018-05-11T21:58:33.000');
INSERT INTO `INPUT` (ID, EVENT_TS) VALUES (2, '2020-05-11T21:58:33.000');
ASSERT VALUES `OUTPUT` (ID, EVENT_TS, ROWTIME) VALUES (1, '2018-05-11T21:58:33.000', 1526075913000);
ASSERT VALUES `OUTPUT` (ID, EVENT_TS, ROWTIME) VALUES (2, '2020-05-11T21:58:33.000', 1589234313000);

--@test: timestampformat - override output timestamp for CSAS
CREATE STREAM TEST (K STRING KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='sink_ts') AS SELECT K, id as sink_ts, id FROM test;
INSERT INTO `TEST` (ID, ROWTIME) VALUES (1, 1526075913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (-2, 1526075913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (3, 1589234313000);
ASSERT VALUES `TS` (SINK_TS, ID, ROWTIME) VALUES (1, 1, 1);
ASSERT VALUES `TS` (SINK_TS, ID, ROWTIME) VALUES (3, 3, 3);

--@test: timestampformat - override output timestamp for CSAS using a string TIMESTAMP_FORMAT
CREATE STREAM TEST (K STRING KEY, ID bigint, EVENT_TS varchar) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='event_ts', timestamp_format='yyyy-MM-dd''T''HH:mm:ssX') AS SELECT K, id, event_ts FROM test;
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (1, '2018-05-11T21:58:33Z', 10);
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (2, 'not a timestamp', 10);
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (3, '2019-05-11T21:58:33Z', 10);
ASSERT VALUES `TS` (ID, EVENT_TS, ROWTIME) VALUES (1, '2018-05-11T21:58:33Z', 1526075913000);
ASSERT VALUES `TS` (ID, EVENT_TS, ROWTIME) VALUES (3, '2019-05-11T21:58:33Z', 1557611913000);

--@test: timestampformat - override output timestamp for CTAS
CREATE TABLE TEST (K STRING PRIMARY KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE TABLE TS WITH (timestamp='sink_ts') AS SELECT K, id as sink_ts, id FROM test;
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('a', 1, 1526075913000);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('a', -2, 1526075913000);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES ('a', 3, 1589234313000);
ASSERT VALUES `TS` (K, SINK_TS, ID, ROWTIME) VALUES ('a', 1, 1, 1);
ASSERT VALUES `TS` (K, SINK_TS, ID, ROWTIME) VALUES ('a', 3, 3, 3);

--@test: timestampformat - override output timestamp for CTAS using a string TIMESTAMP_FORMAT
CREATE STREAM TEST (K STRING KEY, ID bigint, EVENT_TS varchar) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='event_ts', timestamp_format='yyyy-MM-dd''T''HH:mm:ssX') AS SELECT K, id, event_ts FROM test;
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (1, '2018-05-11T21:58:33Z', 10);
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (2, 'not a timestamp', 10);
INSERT INTO `TEST` (ID, EVENT_TS, ROWTIME) VALUES (3, '2019-05-11T21:58:33Z', 10);
ASSERT VALUES `TS` (ID, EVENT_TS, ROWTIME) VALUES (1, '2018-05-11T21:58:33Z', 1526075913000);
ASSERT VALUES `TS` (ID, EVENT_TS, ROWTIME) VALUES (3, '2019-05-11T21:58:33Z', 1557611913000);

--@test: timestampformat - timestamp column of source should not influence sink
CREATE STREAM INPUT (K STRING KEY, ID bigint, EVENT_TS bigint) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='EVENT_TS');
CREATE STREAM OUTPUT AS SELECT K, id as EVENT_TS FROM INPUT;
INSERT INTO `INPUT` (ID, EVENT_TS) VALUES (1, 1526075913000);
INSERT INTO `INPUT` (ID, EVENT_TS) VALUES (2, 1589234313000);
ASSERT VALUES `OUTPUT` (EVENT_TS, ROWTIME) VALUES (1, 1526075913000);
ASSERT VALUES `OUTPUT` (EVENT_TS, ROWTIME) VALUES (2, 1589234313000);

--@test: timestampformat - Invalid timestamp value should throw an exception
CREATE STREAM TEST (K STRING KEY, ID bigint, EVENT_TS string) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='event_ts', timestamp_format='yyyy-MM-dd''T''HH:mm:ssX') AS SELECT K, id, event_ts FROM test;
INSERT INTO `TEST` (ID, EVENT_TS) VALUES (1, 'not a timestamp');


--@test: timestampformat - timestamp with column that does not exist should throw exception
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: The TIMESTAMP column set in the WITH clause does not exist in the schema: 'INVALID_TS'
CREATE STREAM TEST (K STRING KEY, ID bigint, EVENT_TS bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='invalid_ts') AS SELECT id, event_ts FROM test;
--@test: timestampformat - timestamp column with invalid data type should throw exception
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Timestamp column, `EVENT_TS`, should be LONG(INT64), TIMESTAMP, or a String with a timestamp_format specified
CREATE STREAM TEST (K STRING KEY, ID bigint, EVENT_TS int) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='event_ts') AS SELECT K, id, event_ts FROM test;
