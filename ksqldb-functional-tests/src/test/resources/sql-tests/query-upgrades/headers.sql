----------------------------------------------------------------------------------------------------
--@test: should reject sources with multiple HEADERS columns
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Schema already contains a HEADERS column.
----------------------------------------------------------------------------------------------------
CREATE STREAM SOURCE1 (K STRING KEY, V BIGINT, H1 ARRAY<STRUCT<key STRING, value BYTES>> HEADERS, H2 ARRAY<STRUCT<key STRING, value BYTES>> HEADERS) WITH (kafka_topic='stream-source', value_format='json');

----------------------------------------------------------------------------------------------------
--@test: should reject sources with HEADERS and HEADER('key') columns
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Schema already contains a HEADERS column.
----------------------------------------------------------------------------------------------------
CREATE STREAM SOURCE1 (K STRING KEY, V BIGINT, H1 ARRAY<STRUCT<key STRING, value BYTES>> HEADERS, H2 BYTES HEADER('blah')) WITH (kafka_topic='stream-source', value_format='json');

----------------------------------------------------------------------------------------------------
--@test: should reject sources with two HEADER('key') columns with the same key
--@expected.error: io.confluent.ksql.util.KsqlException
--@expected.message: Schema already contains a HEADER('blah') column.
----------------------------------------------------------------------------------------------------
CREATE STREAM SOURCE1 (K STRING KEY, V BIGINT, H1 BYTES HEADER('blah'), H2 BYTES HEADER('blah')) WITH (kafka_topic='stream-source', value_format='json');