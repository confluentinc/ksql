# KLIP-41: Schemas

**Authors**: @derekjn
**Release Target**: N/A |
**Status**: _In Discussion_ |
**Discussion**: --

## Motivation and background

Schemas are a widely employed abstraction across most SQL-based systems. They ultimately provide a layer of hierarchy underneath which logically related groups of objects (i.e. tables, streams, types, etc.) may be organized. In other words, **schemas are SQL’s notion of a namespace**. Similarly to namespaces in other contexts, SQL schemas introduce a level of indirection preceding a given object’s identifier. For example, if an object `my_table` is defined within the schema `my_schema`, the fully qualified object name becomes `my_schema.my_table`

In addition to providing a means to logically organize groups of database objects, schemas also provide basic operational benefits. The most obvious of these benefits is the ability to drop an entire schema in a manner that recursively drops all objects underneath that schema. It is easy to imagine additional benefits that schemas have the potential to provide for ksqlDB users longer term: performing basic operational tasks such as starting and stopping groups of persistent queries, applying configuration properties to an entire schema of objects, and more.

In this KLIP we will propose the initial introduction of schemas into ksqlDB’s grammar, and describe the related functionality that this grammar invokes.

## Scope

### What is in scope

* The overall user experience of ksqlDB schemas will be proposed.
* The minimally necessary grammar required to introduce schemas will be proposed.
* New configuration relating to schemas will be proposed.

### What is not in scope

* The proposed grammar is not exhaustive. It does not include all potential parameters or optional grammar constructs that we may want to support for creating and/or interacting with schemas.
* No implementation details will be proposed.


## Value/return

The most immediate benefit of introducing schemas into ksqlDB is that they will provide a means to better organize workloads in a way that is familiar to users who have experience using other SQL-based systems. Furthermore, as ksqlDB continues to gain widespread adoption, users are increasingly leveraging it to run expansive, complex workloads that would benefit from having more organizational hierarchy.

Additionally, the introduction of a schema abstraction will lay some of the groundwork for enabling users to perform certain operational tasks over groups of database objects in the future. For example, having the ability to [stop and start](https://github.com/confluentinc/ksql/issues/6403) subsets of persistent queries is something we know users want. Schemas have the potential to provide a mechanism for performing such operations efficiently.

## Public APIs

ksqlDB schemas will be primarily leveraged via SQL syntax, with some optional configuration. Each of the new syntax constructs associated with ksqlDB schemas are proposed as follows:

### Creating schemas

```sql
CREATE SCHEMA my_schema;
```

This statement will create a new, empty schema. Once a schema is created, it may be used to qualify any newly created objects:

```sql
CREATE TABLE my_schema.my_table …
CREATE STREAM my_schema.my_stream …
CREATE TYPE my_schema.my_type …
CREATE CONNECTOR my_schema.my_connector …

```

**Any object that can be created and named via `CREATE` should allow an optional schema qualifier.**

### Dropping schemas

```sql
DROP SCHEMA my_schema;
```

If `my_schema` is empty, the above `DROP` statement should succeed, at which point `my_schema` should no longer exist. However, if `my_schema` contains any objects, this statement should fail with an error indicating that schemas that contain objects may not be dropped.

If a user would like to drop a schema that contains any objects, they may use the optional `CASCADE` clause:

```sql
DROP SCHEMA my_schema CASCADE;
```

This statement should recursively drop all objects contained within `my_schema`, and then drop the schema itself.

### Altering an object’s schema (future)

While it would be worth allowing users to change an object’s schema via `ALTER` syntax, `ALTER` is currently unsupported for connectors and types. As a result, we do not need to support object schema modification in the initial implementation of ksqlDB schema support.

Should we decide to support object schema modification in the future, here are some examples of how it should work:

```sql
ALTER TABLE my_table SET SCHEMA my_schema;
ALTER STREAM my_stream SET SCHEMA my_schema;
ALTER TYPE my_type SET SCHEMA my_schema;
ALTER CONNECTOR my_connector SET SCHEMA my_schema;
```

### Configuration

As a matter of convenience and brevity, we should provide a means for users to specify a default schema. If a default schema is given, any references to an object that don’t explicitly include a schema qualifier should be interpreted as targeting the default schema:

```sql
SET 'ksql.default.schema' = 'my_schema';

-- Targets my_schema.my_table:
SELECT my_table …

-- Targets schema1.my_stream:
SELECT schema1.my_stream;

SET 'ksql.default.schema' = 'another_schema';

-- Targets another_schema.my_table:
SELECT my_table ...

```

## Documentation updates

* New documentation will be required to generally describe ksqlDB schemas and their associated syntax.
* Any existing documentation pertaining to naming objects will need to be updated to account for the option of qualifying object names with a schema.

## Compatibility implications

* Once users upgrade from a pre-schemas version of ksqlDB to a version that supports schemas, how will ksqlDB interpret the older version's objects' schemas? 

