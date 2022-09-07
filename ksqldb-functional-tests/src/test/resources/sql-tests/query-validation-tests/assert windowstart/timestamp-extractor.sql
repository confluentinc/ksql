--@test: timestamp-extractor - source default timestamp
CREATE STREAM TEST (K STRING KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS AS SELECT K, id FROM test;
INSERT INTO `TEST` (ID, ROWTIME) VALUES (1, 1526075913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (2, 1557611913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (3, 1589234313000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (1, 1526075913000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (2, 1557611913000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (3, 1589234313000);

--@test: timestamp-extractor - source string timestamp column
CREATE STREAM TEST (K STRING KEY, ID bigint, TS varchar) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='ts', timestamp_format='yy-MM-dd HH:mm:ssX');
CREATE STREAM TS AS SELECT K, id FROM test;
INSERT INTO `TEST` (ID, TS) VALUES (1, '10-04-19 12:00:17Z');
INSERT INTO `TEST` (ID, TS) VALUES (2, '!!!!!!!!!!!!!!!!!');
INSERT INTO `TEST` (ID, TS) VALUES (3, '10-04-19 12:00:17Z');
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (1, 1271678417000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (3, 1271678417000);

--@test: timestamp-extractor - source key timestamp column
CREATE STREAM TEST (K BIGINT KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='K');
CREATE STREAM TS AS SELECT * FROM test;
INSERT INTO `TEST` (K, ID) VALUES (1271678417736, 1);
INSERT INTO `TEST` (K, ID) VALUES (1271678413736, 2);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES (1271678417736, 1, 1271678417736);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES (1271678413736, 2, 1271678413736);

--@test: timestamp-extractor - source key string timestamp column
CREATE STREAM TEST (K STRING KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='K', timestamp_format='yy-MM-dd HH:mm:ssX');
CREATE STREAM TS AS SELECT * FROM test;
INSERT INTO `TEST` (K, ID) VALUES ('10-04-19 12:00:17Z', 1);
INSERT INTO `TEST` (K, ID) VALUES ('10-04-19 12:00:17Z', 2);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES ('10-04-19 12:00:17Z', 1, 1271678417000);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES ('10-04-19 12:00:17Z', 2, 1271678417000);

--@test: timestamp-extractor - source windowed key timestamp column
CREATE STREAM TEST (K BIGINT KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='K', window_type='session');
CREATE STREAM TS AS SELECT * FROM test;
INSERT INTO `TEST` (K, ID, WINDOWSTART, WINDOWEND) VALUES (1271678417736, 1, 1581323504000, 1581323505001);
INSERT INTO `TEST` (K, ID, WINDOWSTART, WINDOWEND) VALUES (1271678413736, 2, 1581323504000, 1581323505001);
ASSERT VALUES `TS` (K, ID, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1271678417736, 1, 1271678417736, 1581323504000, 1581323505001);
ASSERT VALUES `TS` (K, ID, ROWTIME, WINDOWSTART, WINDOWEND) VALUES (1271678413736, 2, 1271678413736, 1581323504000, 1581323505001);

--@test: timestamp-extractor - source throw on invalid timestamp extractor with format
--@expected.error: org.apache.kafka.streams.errors.StreamsException
--@expected.message: Fatal user code error in TimestampExtractor callback for record
SET 'ksql.timestamp.throw.on.invalid' = 'true';CREATE STREAM TEST (K STRING KEY, ID bigint, TS varchar) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='ts', timestamp_format='yy-MM-dd HH:mm:ssX');
CREATE STREAM TS AS SELECT K, id FROM test;
--@test: timestamp-extractor - source bigint timestamp column
SET 'ksql.streams.default.timestamp.extractor' = 'org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp';CREATE STREAM TEST (K STRING KEY, ID bigint, TS bigint) WITH (kafka_topic='test_topic', value_format='JSON', timestamp='ts');
CREATE STREAM TS AS SELECT K, id FROM test;
INSERT INTO `TEST` (ID, TS) VALUES (1, 1526075913000);
INSERT INTO `TEST` (ID, TS) VALUES (2, -1);
INSERT INTO `TEST` (ID, TS) VALUES (3, 1589234313000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (1, 1526075913000);
ASSERT VALUES `TS` (ID, ROWTIME) VALUES (3, 1589234313000);

--@test: timestamp-extractor - sink bigint timestamp column
CREATE STREAM TEST (K STRING KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='sink_ts') AS SELECT K, id as sink_ts, id FROM test;
INSERT INTO `TEST` (ID, ROWTIME) VALUES (1, 1526075913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (-2, 1526075913000);
INSERT INTO `TEST` (ID, ROWTIME) VALUES (3, 1589234313000);
ASSERT VALUES `TS` (SINK_TS, ID, ROWTIME) VALUES (1, 1, 1);
ASSERT VALUES `TS` (SINK_TS, ID, ROWTIME) VALUES (3, 3, 3);

--@test: timestamp-extractor - sink key column
CREATE STREAM TEST (K BIGINT KEY, ID bigint) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM TS WITH (timestamp='K') AS SELECT * FROM test;
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES (1, 100, 1526075913000);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES (-2, 200, 1526075913000);
INSERT INTO `TEST` (K, ID, ROWTIME) VALUES (3, 300, 1589234313000);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES (1, 100, 1);
ASSERT VALUES `TS` (K, ID, ROWTIME) VALUES (3, 300, 3);

