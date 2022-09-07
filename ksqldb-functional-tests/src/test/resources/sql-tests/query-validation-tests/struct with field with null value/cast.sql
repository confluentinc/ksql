--@test: cast - non-array to array
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Select: Cast of STRING to ARRAY<INTEGER> is not supported
CREATE STREAM TEST (ID STRING KEY, f0 VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as ARRAY<INTEGER>) FROM TEST;
--@test: cast - array to string
CREATE STREAM TEST (f0 ARRAY<INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT cast(f0 as STRING) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (ARRAY[1, 3]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('[1, 3]');

--@test: cast - array to array with same schema
CREATE STREAM TEST (ID STRING KEY, f0 ARRAY<INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as ARRAY<INTEGER>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (ARRAY[1, 3]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (ARRAY[1, 3]);

--@test: cast - array<bigint> to array<int>
CREATE STREAM TEST (ID STRING KEY, f0 ARRAY<BIGINT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as ARRAY<INTEGER>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (ARRAY[1, 3, 2147483648]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (ARRAY[1, 3, -2147483648]);

--@test: cast - array<bigint> to array<string>
CREATE STREAM TEST (ID STRING KEY, f0 ARRAY<BIGINT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as ARRAY<STRING>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (ARRAY[1, 3, 2147483648]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (ARRAY['1', '3', '2147483648']);

--@test: cast - array<array<bigint>> to array<array<string>>
CREATE STREAM TEST (ID STRING KEY, f0 ARRAY<ARRAY<BIGINT>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as ARRAY<ARRAY<STRING>>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (ARRAY[ARRAY[1, 3, 2147483648], ARRAY[-1, -3, -2147483648]]);
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (ARRAY[ARRAY['1', '3', '2147483648'], ARRAY['-1', '-3', '-2147483648']]);

--@test: cast - non-map to map
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Select: Cast of STRING to MAP<STRING, INTEGER> is not supported
CREATE STREAM TEST (ID STRING KEY, f0 VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as MAP<VARCHAR, INTEGER>) FROM TEST;
--@test: cast - map to string
CREATE STREAM TEST (f0 MAP<VARCHAR, INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT cast(f0 as STRING) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (MAP('this':=1));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('{this=1}');

--@test: cast - map to map with same schema
CREATE STREAM TEST (ID STRING KEY, f0 MAP<VARCHAR, INTEGER>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as MAP<VARCHAR, INTEGER>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (MAP('this':=1));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (MAP('this':=1));

--@test: cast - map to map with different value type
CREATE STREAM TEST (ID STRING KEY, f0 MAP<STRING, BIGINT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as MAP<STRING, INTEGER>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (MAP('this':=2147483648));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (MAP('this':=-2147483648));

--@test: cast - map<string, map<string, string>> -> map<string, map<string, int>>
CREATE STREAM TEST (ID STRING KEY, f0 MAP<STRING, MAP<STRING, STRING>>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as MAP<STRING, MAP<STRING, INTEGER>>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (MAP('k1':=MAP('k2':='10')));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (MAP('k1':=MAP('k2':=10)));

--@test: cast - non-struct to struct
--@expected.error: io.confluent.ksql.util.KsqlStatementException
--@expected.message: Invalid Select: Cast of STRING to STRUCT<`F0` STRING, `F1` INTEGER> is not supported
CREATE STREAM TEST (ID STRING KEY, f0 VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as STRUCT<F0 VARCHAR, F1 INTEGER>) FROM TEST;
--@test: cast - struct to string
CREATE STREAM TEST (f0 STRUCT<F0 INT, `f1` INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT cast(f0 as STRING) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (STRUCT(f0:=1, `f1`:=3));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('Struct{F0=1,f1=3}');

--@test: cast - struct to struct with same schema
CREATE STREAM TEST (ID STRING KEY, f0 STRUCT<F0 INT, `f1` INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as STRUCT<F0 INTEGER, `f1` INTEGER>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (STRUCT(f0:=1, f1:=3));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (STRUCT(F0:=1, f1:=3));

--@test: cast - struct to struct with different schema
CREATE STREAM TEST (ID STRING KEY, f0 STRUCT<F0 BIGINT, F1 STRING>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as STRUCT<F0 STRING, F3 STRING>) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (STRUCT(f0:=1, f1:='3'));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES (STRUCT(F0:='1', F3:=NULL));

--@test: cast - no op
CREATE STREAM TEST (ID STRING KEY, b BOOLEAN, i INT, bi BIGINT, d DOUBLE, s VARCHAR) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUTPUT AS SELECT ID, cast(b as BOOLEAN), cast(i as INT), cast(bi as BIGINT), cast(d as DOUBLE), cast(s as STRING) FROM TEST;
INSERT INTO `TEST` (B, I, BI, D, S) VALUES (true, 10, 101, 10.3, 'bob');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4) VALUES (true, 10, 101, 10.3, 'bob');

--@test: cast - of nulls
CREATE STREAM TEST (ID STRING KEY, ignored VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(null as BOOLEAN), cast(null as INT), cast(null as BIGINT), cast(null as DOUBLE), cast(null as STRING), cast(null AS ARRAY<INT>), cast(null AS MAP<STRING, INT>), cast(null AS STRUCT<F0 INT>) FROM TEST;
INSERT INTO `TEST` (id) VALUES ('abc');
ASSERT VALUES `OUTPUT` (KSQL_COL_0, KSQL_COL_1, KSQL_COL_2, KSQL_COL_3, KSQL_COL_4, KSQL_COL_5, KSQL_COL_6, KSQL_COL_7) VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

--@test: cast - cast to null
CREATE STREAM TEST (ID STRING KEY, f0 DOUBLE) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, cast(f0 as STRING) as VAL FROM TEST;
INSERT INTO `TEST` (f0) VALUES (NULL);
ASSERT VALUES `OUTPUT` (VAL) VALUES (NULL);

--@test: cast - cast function with error to null without logging error
CREATE STREAM TEST (ID STRING KEY, f0 STRING) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT ID, ifnull(cast(PARSE_DATE(f0, 'yyyy-MM-dd') as string),'1900-01-01') as VAL FROM TEST;
INSERT INTO `TEST` (f0) VALUES ('2022-02-02');
INSERT INTO `TEST` (f0) VALUES (NULL);
INSERT INTO `TEST` (f0) VALUES ('abcd');
ASSERT VALUES `OUTPUT` (VAL) VALUES ('2022-02-02');
ASSERT VALUES `OUTPUT` (VAL) VALUES ('1900-01-01');
ASSERT VALUES `OUTPUT` (VAL) VALUES ('1900-01-01');

--@test: cast - decimal to decimal
CREATE STREAM TEST (ID STRING KEY, foo DECIMAL(4,2)) WITH (kafka_topic='test_topic', value_format='DELIMITED');
CREATE STREAM OUT AS SELECT ID, cast(foo AS DECIMAL(5,3)) FROM TEST;
INSERT INTO `TEST` (FOO) VALUES (10.12);
INSERT INTO `TEST` (FOO) VALUES (01.00);
INSERT INTO `TEST` (FOO) VALUES (00.00);
ASSERT VALUES `OUT` (KSQL_COL_0) VALUES (10.120);
ASSERT VALUES `OUT` (KSQL_COL_0) VALUES (1.000);
ASSERT VALUES `OUT` (KSQL_COL_0) VALUES (0.000);

--@test: cast - integer to decimal
CREATE STREAM TEST (ID STRING KEY, foo INTEGER) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast(foo AS DECIMAL(2,1)) as VAL FROM TEST;
INSERT INTO `TEST` (foo) VALUES (1);
INSERT INTO `TEST` (foo) VALUES (0);
INSERT INTO `TEST` (foo) VALUES (-1);
INSERT INTO `TEST` (foo) VALUES (10);
ASSERT VALUES `OUT` (VAL) VALUES (1.0);
ASSERT VALUES `OUT` (VAL) VALUES (0.0);
ASSERT VALUES `OUT` (VAL) VALUES (-1.0);
ASSERT VALUES `OUT` (VAL) VALUES (NULL);

--@test: cast - double to decimal
CREATE STREAM TEST (ID STRING KEY, foo DOUBLE) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast(foo AS DECIMAL(2,1)) as VAL FROM TEST;
INSERT INTO `TEST` (foo) VALUES (0.1);
INSERT INTO `TEST` (foo) VALUES (1.1);
INSERT INTO `TEST` (foo) VALUES (0.0);
INSERT INTO `TEST` (foo) VALUES (0.99);
INSERT INTO `TEST` (foo) VALUES (0.10);
INSERT INTO `TEST` (foo) VALUES (0.01);
ASSERT VALUES `OUT` (VAL) VALUES (0.1);
ASSERT VALUES `OUT` (VAL) VALUES (1.1);
ASSERT VALUES `OUT` (VAL) VALUES (0.0);
ASSERT VALUES `OUT` (VAL) VALUES (1.0);
ASSERT VALUES `OUT` (VAL) VALUES (0.1);
ASSERT VALUES `OUT` (VAL) VALUES (0.0);

--@test: cast - string to decimal
CREATE STREAM TEST (ID STRING KEY, foo VARCHAR) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast(foo AS DECIMAL(2,1)) as VAL FROM TEST;
INSERT INTO `TEST` (foo) VALUES ('0.1');
INSERT INTO `TEST` (foo) VALUES ('1.1');
INSERT INTO `TEST` (foo) VALUES ('0.0');
INSERT INTO `TEST` (foo) VALUES ('0.99');
INSERT INTO `TEST` (foo) VALUES ('0.10');
INSERT INTO `TEST` (foo) VALUES ('0.01');
ASSERT VALUES `OUT` (VAL) VALUES (0.1);
ASSERT VALUES `OUT` (VAL) VALUES (1.1);
ASSERT VALUES `OUT` (VAL) VALUES (0.0);
ASSERT VALUES `OUT` (VAL) VALUES (1.0);
ASSERT VALUES `OUT` (VAL) VALUES (0.1);
ASSERT VALUES `OUT` (VAL) VALUES (0.0);

--@test: cast - decimal to other
CREATE STREAM TEST (ID STRING KEY, val DECIMAL(4,2)) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast(val AS INT) as i, cast(val AS BIGINT) as l, cast(val as DOUBLE) as d, cast(val AS STRING) as s FROM TEST;
INSERT INTO `TEST` (val) VALUES (00.00);
INSERT INTO `TEST` (val) VALUES (00.01);
INSERT INTO `TEST` (val) VALUES (10.00);
INSERT INTO `TEST` (val) VALUES (10.01);
ASSERT VALUES `OUT` (I, L, D, S) VALUES (0, 0, 0.00, '0.00');
ASSERT VALUES `OUT` (I, L, D, S) VALUES (0, 0, 0.01, '0.01');
ASSERT VALUES `OUT` (I, L, D, S) VALUES (10, 10, 10.00, '10.00');
ASSERT VALUES `OUT` (I, L, D, S) VALUES (10, 10, 10.01, '10.01');

--@test: cast - integer to bigint
CREATE STREAM INPUT (ID STRING KEY, col0 INT, col1 INT) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast((col0 - col1) AS BIGINT) as VAL FROM INPUT;
INSERT INTO `INPUT` (col0, col1) VALUES (1, 2);
ASSERT VALUES `OUT` (VAL) VALUES (-1);

--@test: cast - integer to string
CREATE STREAM INPUT (ID STRING KEY, col0 INT, col1 INT) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast((col0 - col1) AS STRING) as VAL FROM INPUT;
INSERT INTO `INPUT` (col0, col1) VALUES (1, 2);
ASSERT VALUES `OUT` (VAL) VALUES ('-1');

--@test: cast - double to int
CREATE STREAM INPUT (ID STRING KEY, col0 DOUBLE, col1 DOUBLE) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast((col0 - col1) AS INT) as VAL FROM INPUT;
INSERT INTO `INPUT` (col0, col1) VALUES (3.3, 2.1);
ASSERT VALUES `OUT` (VAL) VALUES (1);

--@test: cast - string to timestamp
CREATE STREAM INPUT (ID STRING KEY, col0 STRING) WITH (kafka_topic='test_topic', value_format='AVRO');
CREATE STREAM OUT AS SELECT ID, cast(col0 AS TIMESTAMP) as VAL FROM INPUT;
INSERT INTO `INPUT` (col0) VALUES ('2022-04-28T21:44:47.943');
INSERT INTO `INPUT` (col0) VALUES ('2022-04-28T21:44:47.943Z');
ASSERT VALUES `OUT` (VAL) VALUES ('2022-04-28T21:44:47.943');
ASSERT VALUES `OUT` (VAL) VALUES ('2022-04-28T21:44:47.943');

