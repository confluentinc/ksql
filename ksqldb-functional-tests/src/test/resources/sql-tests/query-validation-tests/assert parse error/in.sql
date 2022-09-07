--@test: in - empty
--@expected.error: io.confluent.ksql.parser.exception.ParseFailedException
--@expected.message: line 2:59: mismatched input ')' expecting
CREATE STREAM INPUT (ID INT KEY, VAL INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN ( );
--@test: in - expressions
CREATE STREAM INPUT (ID INT KEY, VAL INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (VAL, VAL * 2);
INSERT INTO `INPUT` (ID, VAL) VALUES (10, 10);
INSERT INTO `INPUT` (ID, VAL) VALUES (12, 20);
INSERT INTO `INPUT` (ID, VAL) VALUES (11, 110);
INSERT INTO `INPUT` (ID, VAL) VALUES (38, 19);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (10, 10);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (38, 19);

--@test: in - nulls
CREATE STREAM INPUT (ID INT KEY, VAL INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (11,NULL,VAL);
INSERT INTO `INPUT` (VAL) VALUES (10);
INSERT INTO `INPUT` (VAL) VALUES (NULL);
INSERT INTO `INPUT` (ID, VAL) VALUES (11, 10);
INSERT INTO `INPUT` (ID, VAL) VALUES (19, 10);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (11, 10);

--@test: in - inverted
CREATE STREAM INPUT (ID INT KEY, VAL INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID NOT IN (11,20,10);
INSERT INTO `INPUT` (ID) VALUES (10);
INSERT INTO `INPUT` (ID) VALUES (12);
INSERT INTO `INPUT` (ID) VALUES (11);
INSERT INTO `INPUT` (ID) VALUES (19);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (12, NULL);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (19, NULL);

--@test: in - boolean - valid
CREATE STREAM INPUT (ID BOOLEAN KEY, VAL BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (true, 'TrUe', 'tr', 'Y', VAL);
INSERT INTO `INPUT` (ID, VAL) VALUES (true, false);
INSERT INTO `INPUT` (ID, VAL) VALUES (true, true);
INSERT INTO `INPUT` (ID, VAL) VALUES (false, false);
INSERT INTO `INPUT` (ID, VAL) VALUES (false, true);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (true, false);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (true, true);
ASSERT VALUES `OUTPUT` (ID, VAL) VALUES (false, false);

--@test: in - boolean - invalid: non-boolean string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type BOOLEAN: "Not a boolean".
CREATE STREAM INPUT (ID BOOLEAN KEY, VAL BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (true, 'Not a boolean');
--@test: in - boolean - invalid: other literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: BOOLEAN = INTEGER (10)
Hint: You might need to add explicit type casts.
CREATE STREAM INPUT (ID BOOLEAN KEY, VAL BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (true, 10);
--@test: in - boolean - invalid: non-literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: BOOLEAN = STRING (VAL)
Hint: You might need to add explicit type casts.
CREATE STREAM INPUT (ID BOOLEAN KEY, VAL STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (true, VAL);
--@test: in - int - valid
CREATE STREAM INPUT (VAL0 INT, VAL1 BIGINT, VAL2 DOUBLE, VAL3 DECIMAL(4, 2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT VAL0 AS X, VAL0 IN (VAL0) AS VAL0, VAL0 IN (VAL1) AS VAL1, VAL0 IN (VAL2) AS VAL2, VAL0 IN (VAL3) AS VAL3 FROM INPUT WHERE VAL0 IN (1, 2.0, 3.00, '4.000', 5.10, VAl1, VAL2, VAL3);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (1, 0, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (2, 0, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (3, 0, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (4, 0, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (5, 0, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (10, 10, 10.0, 10.00);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (11, 11, 11.01, 11.01);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (-2147483648, 2147483648, 10.0, 10.00);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (1, true, false, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2, true, false, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (3, true, false, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (4, true, false, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (10, true, true, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (11, true, true, false, false);

--@test: in - int - valid long literal
CREATE STREAM INPUT (VAL0 INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (1, '2147483648');
INSERT INTO `INPUT` (VAL0) VALUES (1);
INSERT INTO `INPUT` (VAL0) VALUES (-2147483648);
ASSERT VALUES `OUTPUT` (VAL0) VALUES (1);

--@test: in - int - invalid: non-numeric string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type INTEGER: "10 - not a number".
CREATE STREAM INPUT (ID INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, '10 - not a number');
--@test: in - int - invalid: other literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: INTEGER = BOOLEAN (true)
CREATE STREAM INPUT (ID INT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, true);
--@test: in - int - invalid: non-numeric expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: INTEGER = STRING (VAL0)
CREATE STREAM INPUT (ID INT, VAL0 STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, VAL0);
--@test: in - bigint - valid
CREATE STREAM INPUT (VAL0 INT, VAL1 BIGINT, VAL2 DOUBLE, VAL3 DECIMAL(4, 2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT VAL1 AS X, VAL1 IN (VAL0) AS VAL0, VAL1 IN (VAL1) AS VAL1, VAL1 IN (VAL2) AS VAL2, VAL1 IN (VAL3) AS VAL3 FROM INPUT WHERE VAL1 IN (1, 2.0, 3.00, '4.000', 5.10, VAl0, VAL2, VAL3);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 1, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 2, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 3, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 4, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 5, 0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (10, 10, 10.0, 10.00);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (11, 11, 11.01, 11.01);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (-2147483648, 2147483648, 2147483648, 10.00);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (1, false, true, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2, false, true, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (3, false, true, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (4, false, true, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (10, true, true, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (11, true, true, false, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2147483648, false, true, true, false);

--@test: in - bigint - invalid: non-numeric string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type BIGINT: "10 - not a number".
CREATE STREAM INPUT (ID BIGINT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, '10 - not a number');
--@test: in - bigint - invalid: other literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: BIGINT = BOOLEAN (true)
CREATE STREAM INPUT (ID BIGINT) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, true);
--@test: in - bigint - invalid: non-numeric expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: BIGINT = STRING (VAL0)
CREATE STREAM INPUT (ID BIGINT, VAL0 STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, VAL0);
--@test: in - double - valid
CREATE STREAM INPUT (VAL0 INT, VAL1 BIGINT, VAL2 DOUBLE, VAL3 DECIMAL(4, 2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT VAL2 AS X, VAL2 IN (VAL0) AS VAL0, VAL2 IN (VAL1) AS VAL1, VAL2 IN (VAL2) AS VAL2, VAL2 IN (VAL3) AS VAL3 FROM INPUT WHERE VAL2 IN (1, 2.0, 3.00, '4.000', 5.10, VAl0, VAL1, VAL3);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 1.0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 2.0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 3.0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 4.0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 5.0, 0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (10, 10, 10.0, 10.00);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (11, 11, 11.01, 11.01);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (-2147483648, 2147483648, 2147483648, 10.00);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (1.0, false, false, true, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2.0, false, false, true, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (3.0, false, false, true, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (4.0, false, false, true, false);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (10.0, true, true, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (11.01, false, false, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2147483648, false, true, true, false);

--@test: in - double - invalid: non-numeric string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type DOUBLE: "10.0 - not a number".
CREATE STREAM INPUT (ID DOUBLE) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10.0, '10.0 - not a number');
--@test: in - double - invalid: other literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: DOUBLE = BOOLEAN (true)
CREATE STREAM INPUT (ID DOUBLE) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, true);
--@test: in - double - invalid: non-numeric expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: DOUBLE = STRING (VAL0)
CREATE STREAM INPUT (ID DOUBLE, VAL0 STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, VAL0);
--@test: in - decimal - valid
CREATE STREAM INPUT (VAL0 INT, VAL1 BIGINT, VAL2 DOUBLE, VAL3 DECIMAL(12, 2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT VAL3 AS X, VAL3 IN (VAL0) AS VAL0, VAL3 IN (VAL1) AS VAL1, VAL3 IN (VAL2) AS VAL2, VAL3 IN (VAL3) AS VAL3 FROM INPUT WHERE VAL3 IN (1, 2.0, 3.00, '4.000', 5.10, VAl0, VAL1, VAL2);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 0, 1.0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 0, 2.0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 0, 3.0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 0, 4.0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (0, 0, 0, 5.0);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (10, 10, 10.0, 10.00);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (11, 11, 11.01, 11.01);
INSERT INTO `INPUT` (VAL0, VAL1, VAL2, VAL3) VALUES (-2147483648, 2147483648, 2147483648, 2147483648);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (1.00, false, false, false, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2.00, false, false, false, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (3.00, false, false, false, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (4.00, false, false, false, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (10.00, true, true, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (11.01, false, false, true, true);
ASSERT VALUES `OUTPUT` (X, VAL0, VAL1, VAL2, VAL3) VALUES (2147483648.00, false, true, true, true);

--@test: in - decimal - invalid: non-numeric string literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type DECIMAL: "10.0 - not a number".
CREATE STREAM INPUT (ID DECIMAL(4,2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10.0, '10.0 - not a number');
--@test: in - decimal - invalid: other literal
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: DECIMAL(12, 2) = BOOLEAN (true)
CREATE STREAM INPUT (ID DECIMAL(4,2)) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, true);
--@test: in - decimal - invalid: non-numeric expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: DECIMAL(12, 2) = STRING (VAL0)
CREATE STREAM INPUT (ID DECIMAL(4,2), VAL0 STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, VAL0);
--@test: in - string - valid
CREATE STREAM INPUT (VAL0 STRING, VAL1 STRING) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (true, 10, '10.1', 10.30, VAL1);
INSERT INTO `INPUT` (VAL0) VALUES ('true');
INSERT INTO `INPUT` (VAL0) VALUES ('10');
INSERT INTO `INPUT` (VAL0) VALUES ('10.1');
INSERT INTO `INPUT` (VAL0) VALUES ('10.3');
INSERT INTO `INPUT` (VAL0) VALUES ('10.30');
INSERT INTO `INPUT` (VAL0, VAL1) VALUES ('hello', 'hello');
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES ('true', NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES ('10', NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES ('10.1', NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES ('10.30', NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES ('hello', 'hello');

--@test: in - string - invalid: unsupported expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: STRING = DOUBLE (VAL0)
CREATE STREAM INPUT (ID STRING, VAL0 DOUBLE) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (10, VAL0);
--@test: in - array - valid
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>, VAL1 ARRAY<BIGINT>, VAL2 ARRAY<INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT VAL0 FROM INPUT WHERE VAL0 IN (ARRAY[1,2],ARRAY[5,123456789000],ARRAY['3',null], VAL1, VAL2);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[1, 2]);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[1]);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[1, 2, 3]);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[3, 4]);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (ARRAY[4, 5], ARRAY[4, 5]);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[5, 123456789000]);
INSERT INTO `INPUT` (VAL0) VALUES (ARRAY[3, NULL]);
ASSERT VALUES `OUTPUT` (VAL0) VALUES (ARRAY[1, 2]);
ASSERT VALUES `OUTPUT` (VAL0) VALUES (ARRAY[4, 5]);
ASSERT VALUES `OUTPUT` (VAL0) VALUES (ARRAY[5, 123456789000]);
ASSERT VALUES `OUTPUT` (VAL0) VALUES (ARRAY[3, NULL]);

--@test: in - array - valid expression with wider element type
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>, VAL1 ARRAY<INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL1);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (ARRAY[1, 2], ARRAY[1, 2]);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (ARRAY[2147483648], ARRAY[-2147483648]);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (ARRAY[1, 2], ARRAY[1, 2]);

--@test: in - array - valid expression with narrower element type
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>, VAL1 ARRAY<INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL1 IN (VAL0);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (ARRAY[1, 2], ARRAY[1, 2]);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (ARRAY[2147483648], ARRAY[-2147483648]);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (ARRAY[1, 2], ARRAY[1, 2]);

--@test: in - array - invalid: constructor with non-literal of wrong type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: ARRAY<BIGINT> = ARRAY<BOOLEAN> (ARRAY[VAL1])
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL0, ARRAY[VAL1]);
--@test: in - array - invalid: constructor with incompatible string element
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type BIGINT: "not 10"
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL0, ARRAY['10', 'not 10']);
--@test: in - array - invalid: non-array expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: ARRAY<BIGINT> = INTEGER (ARRAY_LENGTH(VAL0))
CREATE STREAM INPUT (VAL0 ARRAY<BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (ARRAY[1,2], ARRAY_LENGTH(VAL0));
--@test: in - map - valid
CREATE STREAM INPUT (ID MAP<STRING, BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE ID IN (MAP('a':=1), MAP('b':=5,'c':=123456789000), MAP('c':=CAST(null AS BIGINT)));
INSERT INTO `INPUT` (ID) VALUES (MAP('a':=1));
INSERT INTO `INPUT` (ID) VALUES (MAP('a':=1, 'b':=2));
INSERT INTO `INPUT` (ID) VALUES (MAP());
INSERT INTO `INPUT` (ID) VALUES (MAP('b':=5, 'c':=123456789000));
INSERT INTO `INPUT` (ID) VALUES (MAP('c':=NULL));
ASSERT VALUES `OUTPUT` (ID) VALUES (MAP('a':=1));
ASSERT VALUES `OUTPUT` (ID) VALUES (MAP('b':=5, 'c':=123456789000));
ASSERT VALUES `OUTPUT` (ID) VALUES (MAP('c':=NULL));

--@test: in - map - valid: valid expression with wider types
CREATE STREAM INPUT (VAL0 MAP<STRING, BIGINT>, VAL1 MAP<STRING, INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL1 IN (VAL0);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (MAP('a':=1), MAP('a':=1));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (MAP('a':=2147483648), MAP('a':=-2147483648));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (MAP('a':=1), MAP('a':=1));

--@test: in - map - valid: valid expression with narrower types
CREATE STREAM INPUT (VAL0 MAP<STRING, BIGINT>, VAL1 MAP<STRING, INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL1);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (MAP('a':=1), MAP('a':=1));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (MAP('a':=2147483648), MAP('a':=-2147483648));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (MAP('a':=1), MAP('a':=1));

--@test: in - map - invalid: constructor with non-literal of wrong value type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: MAP<STRING, BIGINT> = MAP<STRING, BOOLEAN> (MAP('a':=VAL1))
CREATE STREAM INPUT (VAL0 MAP<STRING, BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL0, MAP('a' := VAL1));
--@test: in - map - invalid: constructor with incompatible string value
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type BIGINT: "not 10"
CREATE STREAM INPUT (VAL0 MAP<STRING, BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL0, MAP('10' := 'not 10'));
--@test: in - map - invalid: non-map expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: MAP<STRING, BIGINT> = INTEGER (10)
CREATE STREAM INPUT (VAL0 MAP<STRING, BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (10);
--@test: in - struct - valid
CREATE STREAM INPUT (VAL0 STRUCT<A BIGINT, B BIGINT>, VAL1 STRUCT<A BIGINT, B BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (STRUCT(A:=1,B:=2),STRUCT(A:=3,B:=2,C:=4),STRUCT(B:=2,A:=4),STRUCT(A:=5),STRUCT(A:=6,B:=CAST(null AS BIGINT)),STRUCT(A:='7',B:=2),VAL1);
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=1, B:=2));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=2, B:=2));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=3, B:=2));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=4, B:=2));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=5));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=6));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(A:=7, B:=2));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (STRUCT(A:=8, B:=2), STRUCT(A:=8, B:=2));
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=1, B:=2), NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=4, B:=2), NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=5, B:=NULL), NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=6, B:=NULL), NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=7, B:=2), NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(A:=8, B:=2), STRUCT(A:=8, B:=2));

--@test: in - struct - valid expression with wider type
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>, VAL1 STRUCT<F0 INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL1);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2), STRUCT(F0:=2));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2147483648), STRUCT(F0:=-2147483648));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2), STRUCT(F0:=2));

--@test: in - struct - valid expression with narrower type
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>, VAL1 STRUCT<F0 INT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL1 IN (VAL0);
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2), STRUCT(F0:=2));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2147483648), STRUCT(F0:=-2147483648));
INSERT INTO `INPUT` (VAL0, VAL1) VALUES (NULL, NULL);
ASSERT VALUES `OUTPUT` (VAL0, VAL1) VALUES (STRUCT(F0:=2), STRUCT(F0:=2));

--@test: in - struct - invalid: constructor with non-literal of wrong type
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: STRUCT<`F0` BIGINT> = STRUCT<`F0` BOOLEAN> (STRUCT(F0:=VAL1))
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (VAL0, STRUCT(F0 := VAL1));
--@test: in - struct - invalid: constructor with incompatible string element
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: invalid input syntax for type BIGINT: "not 10"
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>, VAL1 BOOLEAN) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (STRUCT(F0 := 'not 10'));
--@test: in - struct - invalid: non-struct expression
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Predicate: operator does not exist: STRUCT<`F0` BIGINT> = ARRAY<INTEGER> (ARRAY[10])
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (STRUCT(F0 := 10), ARRAY[10]);
--@test: in - struct - field names are case sensitive
CREATE STREAM INPUT (VAL0 STRUCT<F0 BIGINT>) WITH (kafka_topic='input_topic', format='JSON');
CREATE STREAM OUTPUT AS SELECT * FROM INPUT WHERE VAL0 IN (STRUCT(F0 := 10), STRUCT(`f0` := 20));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(F0:=10));
INSERT INTO `INPUT` (VAL0) VALUES (STRUCT(F0:=20));
ASSERT VALUES `OUTPUT` (VAL0) VALUES (STRUCT(F0:=10));

