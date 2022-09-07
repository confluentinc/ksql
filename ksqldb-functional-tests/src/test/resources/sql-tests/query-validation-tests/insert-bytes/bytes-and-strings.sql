--@test: bytes-and-strings - convert bytes to HEX encoded string
CREATE STREAM TEST (s STRING, b BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select FROM_BYTES(b, 'hex') AS hex, TO_BYTES(FROM_BYTES(b, 'hex'), 'hex') AS b1, FROM_BYTES(TO_BYTES(s, 'hex'), 'hex') AS s1 from test;
INSERT INTO `TEST` (S, B) VALUES ('21', 'IQ==');
INSERT INTO `TEST` (S, B) VALUES ('23', 'Iw==');
INSERT INTO `TEST` (S, B) VALUES ('1aB2', 'GrI=');
ASSERT VALUES `TS` (HEX, B1, S1) VALUES ('21', 'IQ==', '21');
ASSERT VALUES `TS` (HEX, B1, S1) VALUES ('23', 'Iw==', '23');
ASSERT VALUES `TS` (HEX, B1, S1) VALUES ('1AB2', 'GrI=', '1AB2');
ASSERT stream TS (hex STRING, b1 BYTES, s1 STRING) WITH (KAFKA_TOPIC='TS');

--@test: bytes-and-strings - convert bytes to ASCII encoded string
CREATE STREAM TEST (s STRING, b BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select FROM_BYTES(b, 'ascii') AS ascii, TO_BYTES(FROM_BYTES(b, 'ascii'), 'ascii') AS b1, FROM_BYTES(TO_BYTES(s, 'ascii'), 'ascii') AS s1 from test;
INSERT INTO `TEST` (S, B) VALUES ('!', 'IQ==');
INSERT INTO `TEST` (S, B) VALUES ('#', 'Iw==');
ASSERT VALUES `TS` (ASCII, B1, S1) VALUES ('"!"', 'IQ==', '"!"');
ASSERT VALUES `TS` (ASCII, B1, S1) VALUES ('"#"', 'Iw==', '"#"');
ASSERT stream TS (ascii STRING, b1 BYTES, s1 STRING) WITH (KAFKA_TOPIC='TS');

--@test: bytes-and-strings - convert bytes to UTF8 encoded string
CREATE STREAM TEST (s STRING, b BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select FROM_BYTES(b, 'utf8') AS utf8, TO_BYTES(FROM_BYTES(b, 'utf8'), 'utf8') AS b1, FROM_BYTES(TO_BYTES(s, 'utf8'), 'utf8') AS s1 from test;
INSERT INTO `TEST` (S, B) VALUES ('!', 'IQ==');
INSERT INTO `TEST` (S, B) VALUES ('#', 'Iw==');
ASSERT VALUES `TS` (UTF8, B1, S1) VALUES ('"!"', 'IQ==', '"!"');
ASSERT VALUES `TS` (UTF8, B1, S1) VALUES ('"#"', 'Iw==', '"#"');
ASSERT stream TS (utf8 STRING, b1 BYTES, s1 STRING) WITH (KAFKA_TOPIC='TS');

--@test: bytes-and-strings - convert bytes to BASE64 encoded string
CREATE STREAM TEST (s STRING, b BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select FROM_BYTES(b, 'base64') AS base64, TO_BYTES(FROM_BYTES(b, 'base64'), 'base64') AS b1, FROM_BYTES(TO_BYTES(s, 'base64'), 'base64') AS s1 from test;
INSERT INTO `TEST` (S, B) VALUES ('IQ==', 'IQ==');
INSERT INTO `TEST` (S, B) VALUES ('Iw==', 'Iw==');
ASSERT VALUES `TS` (BASE64, B1, S1) VALUES ('IQ==', 'IQ==', 'IQ==');
ASSERT VALUES `TS` (BASE64, B1, S1) VALUES ('Iw==', 'Iw==', 'Iw==');
ASSERT stream TS (base64 STRING, b1 BYTES, s1 STRING) WITH (KAFKA_TOPIC='TS');

--@test: bytes-and-strings - convert bytes to INT, BIGINT, and DOUBLE using BIG ENDIAN
CREATE STREAM TEST (a BYTES, b BYTES, c BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select INT_FROM_BYTES(a) as a, BIGINT_FROM_BYTES(b) as b, DOUBLE_FROM_BYTES(c) AS c from test;
INSERT INTO `TEST` (A, B, C) VALUES ('AAAH5Q==', 'AAAAASoF8gA=', 'QICm/ZvJ9YI=');
ASSERT VALUES `TS` (A, B, C) VALUES (2021, 5000000000, 532.8738323);
ASSERT stream TS (a INT, b BIGINT, c DOUBLE) WITH (KAFKA_TOPIC='TS');

--@test: bytes-and-strings - convert bytes to INT, BIGINT, and DOUBLE using LITTLE ENDIAN
CREATE STREAM TEST (a BYTES, b BYTES, c BYTES) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM TS AS select INT_FROM_BYTES(a, 'LITTLE_ENDIAN') as a, BIGINT_FROM_BYTES(b, 'LITTLE_ENDIAN') as b, DOUBLE_FROM_BYTES(c, 'LITTLE_ENDIAN') AS c from test;
INSERT INTO `TEST` (A, B, C) VALUES ('5QcAAA==', 'APIFKgEAAAA=', 'gvXJm/2mgEA=');
ASSERT VALUES `TS` (A, B, C) VALUES (2021, 5000000000, 532.8738323);
ASSERT stream TS (a INT, b BIGINT, c DOUBLE) WITH (KAFKA_TOPIC='TS');

