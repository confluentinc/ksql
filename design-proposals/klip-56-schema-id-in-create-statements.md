# KLIP 56 - Schema ID in Create Statements

**Author**: Hao Li (@lihaosky) | 
**Release Target**: 0.24 | 
**Status**: _Merged_ | 
**Discussion**: [Issue](https://github.com/confluentinc/ksql/issues/3634), [GitHub PR1](https://github.com/confluentinc/ksql/pull/8177)

**tl;dr:** This KLIP introduces `key_schema_id` and `value_schema_id` in `C*AS` statements and details the expected behaviors in various commands.

## Motivation and background

As discussed in this [issue](https://github.com/confluentinc/ksql/issues/3634), the main motivation is to enable users to specify `key_schema_id` or `value_schema_id` in `CAS` and `CS/CT` statements so that existing schemas in _schema registry_ can be reused for logical schema creation and data serialization for the newly defined data source.

Major benefits are:
* Let users easily reuse existing schemas in _schema registry_ without rewriting the schemas in `CREATE` command again which could be error-prone.
* By using the schema IDs, the user can enforce conventions like field name casing, for example to ensure ksqlDB doesn't capitalize the field names which may create incompatibility between schemas later.
* User can use predefined schema in _schema registry_ to serialize data which can be used by downstreams who consume data using the same schema.

## What is in scope
* Fix _ksqlDB_ to not always uppercase field names in schema conversions between _schema registry_ schemas and ksqlDB schemas.
* Support `KEY_SCHEMA_FULL_NAME` and `VALUE_SCHEMA_FULL_NAME` properties to control full schema name for _AVRO_ and _PROTOBUF_ format.
* Support `key_schema_id` and `value_schema_id` in `CAS` and `CS/CT` statements with proper validations.
* Add support in _ksqlDB_ to serialize data using existing schema defined in _schema registry_ by specifying schema ID.

## What is not in scope
* _ksqlDB_ support schema inference if no _table elements_ and _schema ID_ are provided in `CS/CT` statement. The behavior of this schema inference will not change.
* Alert user of potential logical schema and physical schema incompatibility in stream/table creation time. Because _ksqlDB_ logical schema fields are all nullable whereas the provided schema using schema ID may not have all fields to be nullable. We won't do this compatibility check during creation time. Error will occur if schemas are not compatible during value insertion time.

## Public APIS
### Schema IDs in `CAS` statement
```roomsql
CREATE STREAM stream_name WITH (key_schema_id=1, value_schema_id=2, key_format='avro', value_format='avro') AS 
SELECT key, field1, field_2 FROM source_stream
EMIT CHANGES;
```
```roomsql
CREATE TABLE table_name WITH (key_schema_id=1, value_schema_id=2, key_format='avro', value_format='avro') AS 
SELECT key, COUNT(field1) AS count_field FROM source_stream
GROUP BY key;
```

### Schema IDs in `CS/CT`
```roomsql
CREATE STREAM stream_name WITH (kafka_topic='topic_name', key_schema_id=1, value_schema_id=2, 
partitions=1, key_format='avro', value_format='avro');
```
```roomsql
CREATE TABLE table_name WITH (kafka_topic='table_name', key_schema_id=1, value_schema_id=2, 
partitions=1, key_format='avro', value_format='avro');
```

## Design
### Validations needed when `*_schema_id` presents
* `CS/CT` command
  * Corresponding `key_format`/`value_format` or `format` property must exist and the format must support `SCHEMA_INFERENCE` (protobuf, avro, json_sr format currently).
  * The fetched schema format from _schema_registry_ must match specified format in `WITH` clause. For example, if schema format for schema ID in _schema_registry_ is `avro` but specified format in `WITH` clause is `protobuf`, an exception will be thrown.
  * Schema with specified ID MUST exist in _schema_registry_, otherwise an exception will be thrown. 
  * Corresponding key/value _table elements_ must be empty if `key_schema_id` or `value_schema_id` is provided.

* `CAS` command
  * For `key_format` and `value_format` properties, if `*_schema_id` is provided:
    * If a _format_ property is provided, it must match the format fetched using schema ID.
    * If a _format_ property is not provided, it will be deduced from query source's format and then must match the format fetched using schema ID.
  * Schema with specified ID MUST exist in _schema_registry_, otherwise an exception will be thrown.
  * Compatibility check. Users can use `*_schema_id` with select projection in `CAS` command. For example, `CREATE STREAM stream_name WITH (kafka_topic='topic_name', key_schema_id=1, value_schema_id=2,
    partitions=1, key_format='avro', value_format='avro') AS SELECT * FROM source_stream;`. In this situation, schema from _schema registry_ should be a superset of select projection and the fields order must match. Note that when compatibility is checked, whether a field is optional or required 
    in physical schema doesn't matter. This can give user more flexibility to use more schemas. Otherwise, fields schema can only be optional since fields in _ksqlDB_ are all optional. It is also allowed for schema
    from _schema registry_ to have more fields. We currently don't mandate those fields to be optional. In case extra fields are required, serialization error might occur during value insertion time since value schema will be based query
    projection schema. We expect users who use schema ID to be advanced users and responsible to make sure the serialization can work.

### _ksqlDB_ logical schema creation and physical schema registration
* Schema specified using schema ID will be looked up and fetched from _schema registry_.
* It will then be translated to _ksqlDB_ columns:
  * If SerdeFeatures `UNWRAP` is specified in the creation statement, single key or value column named `ROWKEY` or `ROWVALUE` will be created and fetched schema will be translated to key or value's type.
  * Otherwise, fetched schema is expected to be `STRUCT` type and field names will be _ksqlDB_ column names. Schema for each field will be corresponding column's type.
  * If there are unsupported types or other translation errors, statement will fail.
* If columns translation is successful.
  * For `CS/CT` statement, the translated columns will be injected to original statement's _table elements_ and registered when DDL command is executed.
  * For `CAS` statement, compatibility check will be done against the schema of query projection. **Logical schema of created stream/table will still be the schema of query projection** which is different from
    the behavior in `CS/CT` statement.
* Then the physical schema will be registered in _schema registry_ under correct _ksqlDB_subject name. For example, in `CS/CT` statement, fetched physical schema wil be
  registered under `[topic]-key` or `[topic]-value` subject. Fetched physcial schema is registered again because we can do schema compatibility checks if we create other
  sources using the same topic.
  
### Data write path
* Schema ID defined in source creation statement will be stored in _ksqlDB_.
* During data serialization time, data will be created with source logical schema first and the data will be rewritten following the physical schema fetched using schema ID.
  * Data rewrite could fail at this time if logical schema and physical schema are not compatible. For example, if a field defined in physical schema is required but can be optional in _ksqlDB_. If `null` is field
    value, rewrite using physical schema will fail.
* After data rewrite using physical schema, data can be serialized using corresponding format's serializer.
  * Note that the underlying serializer will be configured to use schema ID we provided to do schema lookup and use the physical schema to do serialization[^1].

### Data read path
* Data will be deserialized using physical schema used for serialization.
* When data are converted to _ksqlDB_ data, it will be converted using source's logical schema.
  * Since physical schema is a superset of logical schema and logical schema's fields are all optional, we expect data can be successfully converted always.

## Test plan
Tests for the following:
* Schema translator doesn't capitalize field names if schema ID is set in source creation statements.
* Schemas are correctly validated when `*_schema_id` presents.
* Different format of schemas can be registered in both `CAS` and `CS/CT` statements.
* Data can be serialized/deserialized for both `CAS` and `CS/CT` statements if physical schema is compatible with logical schema.
* Data can not be serialized if physical schema contains required field whose value is provided as null.
 

## Documentation Updates
Add newly supported properties to:
* `docs/developer-guide/ksqldb-reference/create-stream.md`
* `docs/developer-guide/ksqldb-reference/create-table.md`
* `docs/developer-guide/ksqldb-reference/create-stream-as-select.md`
* `docs/developer-guide/ksqldb-reference/create-table-as-select.md`

## Compatibility Implications
`key_schema_id` and `value_schema_id` properties exists in ksqlDB codebase and it's possible to issue `CREATE` statement with them. There could be commands which already used them but those commands should be augmented and written to the command topic without the properties. As a result, adding more validation checks or changing how the properties should be handled have no compatibility issue.

## Security Implications
No

## References
[^1]: [Avro Kafka Serializer](https://github.com/confluentinc/schema-registry/blob/802dc1a4dfbca7919375e28dfe2cbe793ae57e0c/avro-serializer/src/main/java/io/confluent/kafka/serializers/AbstractKafkaAvroSerializer.java#L116)
