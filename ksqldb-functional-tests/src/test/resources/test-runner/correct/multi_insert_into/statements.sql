CREATE STREAM S1 (ID bigint, NAME varchar, VALUE bigint) WITH (kafka_topic='s1', partitions=1, value_format='JSON');

CREATE STREAM S2 (ID bigint, NAME varchar, VALUE bigint)
    WITH (kafka_topic='s2', partitions=1, value_format='JSON');

CREATE STREAM S3 (ID bigint, NAME varchar, VALUE bigint, SOURCE varchar)
    WITH (kafka_topic='s3', partitions=1, value_format='JSON');

INSERT INTO s3 SELECT id, name, value, 's1' AS source FROM s1;

INSERT INTO s3 SELECT id, name, value, 's2' AS source FROM s2;