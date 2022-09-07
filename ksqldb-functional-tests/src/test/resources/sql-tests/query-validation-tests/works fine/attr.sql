--@test: attr - valid attr with various scalar fields
CREATE STREAM input (k INT KEY, s STRING, i INT, bi BIGINT, d DOUBLE, bo BOOLEAN) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(s) AS s, ATTR(i) AS i, ATTR(bi) AS bi, ATTR(d) AS d, ATTR(bo) AS bo FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s, i, bi, d, bo) VALUES (1, 's', 1, 1, 1.0, true);
INSERT INTO `INPUT` (K, s, i, bi, d, bo) VALUES (1, 's', 1, 1, 1.0, true);
INSERT INTO `INPUT` (K, s, i, bi, d, bo) VALUES (1, 's', 1, 1, 1.0, true);
ASSERT VALUES `OUT` (K, S, I, BI, D, BO) VALUES (1, 's', 1, 1, 1.0, true);
ASSERT VALUES `OUT` (K, S, I, BI, D, BO) VALUES (1, 's', 1, 1, 1.0, true);
ASSERT VALUES `OUT` (K, S, I, BI, D, BO) VALUES (1, 's', 1, 1, 1.0, true);

--@test: attr - valid attr with various non-scalar fields
CREATE STREAM input (k INT KEY, li ARRAY<INT>, s STRUCT<A STRING, B INT>) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(li) AS li, ATTR(s) AS s FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s, li) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (K, s, li) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (K, s, li) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);
ASSERT VALUES `OUT` (K, S, LI) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);
ASSERT VALUES `OUT` (K, S, LI) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);
ASSERT VALUES `OUT` (K, S, LI) VALUES (1, STRUCT(A:='a', B:=1), ARRAY[1, 2, 3]);

--@test: attr - valid attr with nulls
CREATE STREAM input (k INT KEY, s STRING) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(s) AS s FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s) VALUES (1, NULL);
INSERT INTO `INPUT` (K, s) VALUES (1, NULL);
INSERT INTO `INPUT` (K, s) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);

--@test: attr - invalid attr
CREATE STREAM input (k INT KEY, s STRING) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(s) AS s FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s) VALUES (1, '1');
INSERT INTO `INPUT` (K, s) VALUES (1, '2');
INSERT INTO `INPUT` (K, s) VALUES (1, '3');
ASSERT VALUES `OUT` (K, S) VALUES (1, '1');
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);

--@test: attr - invalid attr with nulls
CREATE STREAM input (k INT KEY, s STRING) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(s) AS s FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s) VALUES (1, '1');
INSERT INTO `INPUT` (K, s) VALUES (1, NULL);
INSERT INTO `INPUT` (K, s) VALUES (1, '1');
ASSERT VALUES `OUT` (K, S) VALUES (1, '1');
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);

--@test: attr - invalid attr with nulls as first entry
CREATE STREAM input (k INT KEY, s STRING) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT k, ATTR(s) AS s FROM input GROUP BY k;
INSERT INTO `INPUT` (K, s) VALUES (1, NULL);
INSERT INTO `INPUT` (K, s) VALUES (1, '2');
INSERT INTO `INPUT` (K, s) VALUES (1, '2');
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (K, S) VALUES (1, NULL);

--@test: attr - table attr with tombstones
CREATE TABLE input (k INT PRIMARY KEY, g INT, s STRING) WITH (kafka_topic='in', value_format='JSON');
CREATE TABLE out AS SELECT g, ATTR(s) AS s FROM input GROUP BY g;
INSERT INTO `INPUT` (K) VALUES (1);
INSERT INTO `INPUT` (K, g, s) VALUES (1, 1, '1');
INSERT INTO `INPUT` (K, g, s) VALUES (2, 1, '1');
INSERT INTO `INPUT` (K) VALUES (1);
INSERT INTO `INPUT` (K) VALUES (2);
INSERT INTO `INPUT` (K, g, s) VALUES (1, 1, '2');
INSERT INTO `INPUT` (K, g, s) VALUES (2, 1, '3');
INSERT INTO `INPUT` (K) VALUES (2);
ASSERT VALUES `OUT` (G, S) VALUES (1, '1');
ASSERT VALUES `OUT` (G, S) VALUES (1, '1');
ASSERT VALUES `OUT` (G, S) VALUES (1, '1');
ASSERT VALUES `OUT` (G, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (G, S) VALUES (1, '2');
ASSERT VALUES `OUT` (G, S) VALUES (1, NULL);
ASSERT VALUES `OUT` (G, S) VALUES (1, '2');

