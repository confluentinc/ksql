# KLIP 56 - Schema ID in Create Statements

**Author**: Hao Li (@lihaosky) | 
**Release Target**: 0.24 | 
**Status**: _In Discussion_ | 
**Discussion**: [Issue](https://github.com/confluentinc/ksql/issues/3634), [GitHub PR1](https://github.com/confluentinc/ksql/pull/8177)

**tl;dr:** This KLIP introduces `key_schema_id` and `value_schema_id` in `C*AS` statements and details the expected behaviors in various commands.

## Motivation and background

As discussed in this [issue](https://github.com/confluentinc/ksql/issues/3634), the main motivation is to enable users to specify `key_schema_id` or `value_schema_id` in `C*AS` and `CS/CT` commands so that existing schemas in _schema registry_ can be reused in new `CREATE` command.

Major benefits are:
* Let users easily reuse existing schemas in _schema registry_ without rewriting the schemas in `CREATE` command again which could be error-prone.
* By using the schema IDs, the user can enforce conventions like field name casing, for example to ensure ksqlDB doesn't uppercase the field names which may create incompatibility between schemas later.

## What is in scope
* Fix always uppercase field names in schema conversions between _schema registry_ schemas and ksqlDB schemas.
* Add support of `key_schema_id` and `value_schema_id` in `C*AS` statements with proper validations.
* Add proper validation for `key_schema_id` and `value_schema_id` in `CS/CT` statements.
* Provide compatibility checks of schemas from schema id and schemes from table elements in `CREATE * WITH` or select projection in `C*AS`.

## What is not in scope
* Schema creation in existing subjects in _schema registry_ will always succeed without compatibility check in _schema registry_. Compatibility check is only done during `INSERT` time which could result in always failed insertion. This fix is not covered in this KLIP.

## Public APIS
### Schema ids in `C*AS`
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

### Schema ids in `CREATE * WITH`
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
* `CREATE * WITH` command
  * Corresponding `key_format` or `value_format` must exist and the format must support `SCHEMA_INFERENCE` (protobuf, avro, json_sr format currently).
  * The fetched schema format from _schema_registry_ must match specified format in `WITH` clause. For example, if schema format for schema id in _schema_registry_ is `avro` but specified format in `WITH` clause is `protobuf`, an exception will be thrown.
  * Schema with specified ID MUST exist in _schema_registry_, otherwise an exception will be thrown.
  * Serde Features must be`WRAPPED` so that field names always exist. The reason is that if Serde Features has `UNWRAP_SINGLES` enabled, during translation, anonymous `ROWKEY` or `ROWVALUE` column name will be inserted for the schema first which makes the translated schema diverging from original schema in _Schema Registry_.
  * Compatibility Checks. See section below.

* `C*AS` command
  * `key_format` and `value_format` are optional. If specified, they must match format fetched from _schema_registry_.
  * Schema with specified ID MUST exist in _schema_registry_, otherwise an exception will be thrown.
  * Serde Features must be `WRAPPED` so that field names always exist.
  * Compatibility Checks. See section below.

### Compatibility checks
* Users can use `*_schema_id` with table elements in `CS/CT` command. For example, `CREATE STREAM stream_name (id INT KEY, value VARCHAR) with (kafka_topic='topic_name, value_schema_id=1, value_format='avro');`. In this case, schema fetched from _schema registry_ should be a superset of table elements and schema from _schema registry_ will be used as new schema.
* Users can use `*_schema_id` with select projection in `C*AS` command. For example, `CREATE STREAM stream_name WITH (kafka_topic='topic_name', key_schema_id=1, value_schema_id=2,
  partitions=1, key_format='avro', value_format='avro') AS SELECT * FROM source_stream;`. In this situation, schema from _schema registry_ should a superset of select projection and the extra fields should be optional fields or required fields with default value. This can make sure the new schema can be compatible with select projection schema which may have fewer fields.

### Schema registration flow
* Schema lookup: schema will be looked up from _schema_registry_ using provided `*_schema_id`.
* Schema validation: using validation described in previous sections.
* Schema translation: translate parsed _schema_registry_ schema to `ksqlDB` schema and verify they are the same syntactically.
* Schema creation: exact same schema will be created and registered in _ksqlDB_ and _schema_registry_ under correct subject name.


## Test plan
Tests for the following:
* Schema translator doesn't always upppercase field names.
* Schemas are correctly validated when `*_schema_id` presents.
* Different format of schemas can be registered in both `C*AS` and `CS/CT` commands.
 

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


