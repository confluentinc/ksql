--@test: extract-json-field - concat two extracted fields
CREATE STREAM TEST (K STRING KEY, source VARCHAR) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, EXTRACTJSONFIELD(source, '$.name') AS SOURCE, EXTRACTJSONFIELD(source, '$.version') AS VERSION, CONCAT(EXTRACTJSONFIELD(source, '$.name'), EXTRACTJSONFIELD(source, '$.version')) AS BOTH, EXTRACTJSONFIELD(source, '$.@type') AS TYPE FROM TEST;
INSERT INTO `TEST` (source) VALUES ('{"name": "cdc", "version": "1", "@type": "UDF"}');
INSERT INTO `TEST` (source) VALUES ('{"name": "cdd", "version": "2", "@type": "VAL"}}');
ASSERT VALUES `OUTPUT` (SOURCE, VERSION, BOTH, TYPE) VALUES ('cdc', '1', 'cdc1', 'UDF');
ASSERT VALUES `OUTPUT` (SOURCE, VERSION, BOTH, TYPE) VALUES ('cdd', '2', 'cdd2', 'VAL');

--@test: extract-json-field - extract JSON array field
CREATE STREAM TEST (K STRING KEY, array_field ARRAY<VARCHAR>) WITH (kafka_topic='test_topic', value_format='JSON');
CREATE STREAM OUTPUT AS SELECT K, EXTRACTJSONFIELD(array_field[1], '$.nested') AS Col1, EXTRACTJSONFIELD(array_field[2], '$.nested') AS Col2 FROM TEST;
INSERT INTO `TEST` (array_field) VALUES (ARRAY['{"nested": "nest0"}', '{"nested": "nest1"}']);
INSERT INTO `TEST` (array_field) VALUES (ARRAY['{"nested": "nest0"}']);
INSERT INTO `TEST` (array_field) VALUES (ARRAY[]);
ASSERT VALUES `OUTPUT` (COL1, COL2) VALUES ('nest0', 'nest1');
ASSERT VALUES `OUTPUT` (COL1, COL2) VALUES ('nest0', NULL);
ASSERT VALUES `OUTPUT` (COL1, COL2) VALUES (NULL, NULL);

--@test: extract-json-field - array bounds
CREATE STREAM INPUT (K STRING KEY, json STRING) WITH (kafka_topic='test_topic', value_format='KAFKA');
CREATE STREAM OUTPUT WITH(value_format='JSON') AS SELECT K, EXTRACTJSONFIELD(json, '$.array[-1]') AS Col1, EXTRACTJSONFIELD(json, '$.array[0]') AS Col2, EXTRACTJSONFIELD(json, '$.array[1]') AS Col3 FROM INPUT;
INSERT INTO `INPUT` (JSON) VALUES ('{"array": [1, 2]}');
INSERT INTO `INPUT` (JSON) VALUES ('{"array": [1.23450]}');
ASSERT VALUES `OUTPUT` (COL1, COL2, COL3) VALUES (NULL, '1', '2');
ASSERT VALUES `OUTPUT` (COL1, COL2, COL3) VALUES (NULL, '1.23450', NULL);

