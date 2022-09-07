--@test: operators - precedence: asterisk before plus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 2 * 3 + 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (10);

--@test: operators - precedence: asterisk before minus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 2 * 3 - 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (2);

--@test: operators - precedence: slash before plus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 6 / 3 + 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (6);

--@test: operators - precedence: slash before minus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 6 / 3 - 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (-2);

--@test: operators - precedence: percent before plus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 6 % 4 + 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (6);

--@test: operators - precedence: percent before minus
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 16 % 6 - 4 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (0);

--@test: operators - precedence: plus and minus by order
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 10 - 2 + 5 AS VAL, 5 + 10 - 2 AS VAL2 FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL, VAL2) VALUES (13, 13);

--@test: operators - precedence: percent, slash and asterisk by order
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 6 % 4 / 2 * 6 AS VAL, 6 / 2 % 2 * 2 AS VAL2, 2 * 3 % 4 AS VAL3 FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL, VAL2, VAL3) VALUES (6, 2, 2);

--@test: operators - precedence: plus and minus before comparison
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 10 + 4 > 11 - 2 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

--@test: operators - precedence: comparison before NOT
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT NOT 4 > 11 AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

--@test: operators - precedence: NOT before AND
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT NOT false AND NOT false AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

--@test: operators - precedence: AND before BETWEEN
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 1 BETWEEN 0 AND 2 AND 'b' BETWEEN 'a' AND 'c' AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

--@test: operators - precedence: AND before LIKE
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT 'this' LIKE '%hi%' AND true AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

--@test: operators - precedence: AND before OR
CREATE STREAM INPUT (IGNORED INT) WITH (kafka_topic='input', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT true OR false AND true AS VAL FROM INPUT;
INSERT INTO `INPUT` (ignored) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (true);

