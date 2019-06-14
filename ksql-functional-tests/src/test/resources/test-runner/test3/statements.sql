CREATE STREAM TEST (ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='left_topic', value_format='JSON', key='ID');
CREATE STREAM TEST_STREAM (ID bigint, F1 varchar, F2 bigint) WITH (kafka_topic='right_topic', value_format='JSON', key='ID');
CREATE STREAM LEFT_OUTER_JOIN as SELECT t.id, name, value, f1, f2 FROM test t left join TEST_STREAM tt WITHIN 11 seconds ON t.id = tt.id;
CREATE STREAM foo AS SELECT t_id, name FROM LEFT_OUTER_JOIN WHERE t_id = 90;
CREATE STREAM bar AS SELECT * FROM foo;