--@test: cast - struct to string
CREATE STREAM TEST (f0 STRUCT<x INT, f1 INT>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT cast(f0 as STRING) FROM TEST;
INSERT INTO `TEST` (f0) VALUES (STRUCT(x:=1, f1:=3));
ASSERT VALUES `OUTPUT` (KSQL_COL_0) VALUES ('Struct{X=1,F1=3}');