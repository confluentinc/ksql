  # KLIP-4: Custom Type Registry

**Author**: agavra | 
**Release Target**: 5.4 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/2894

**tl;dr:** *Introduce a feature that makes custom types easier to work with in KSQL by aliasing
complex type declarations.*

## Motivation and background

Kafka often contains very complex nested data structures and it is common to see schemas nested with
other schemas. It is desirable to be able to define this custom schema in KSQL once and reference it
in all future statements. For example, imagine the following schema declarations:

```sql
CREATE TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
CREATE TYPE PERSON AS STRUCT<firstname VARCHAR, lastname VARCHAR, address ADDRESS>;
CREATE TYPE COMPANY AS STRUCT<name VARCHAR, headquarters ADDRESS>;
```

With this feature organizations can leverage existing schemas in their KSQL queries and share 
data with other parts of their organizations.

## Scope

* Syntax to register and unregister custom type aliases
* Ability to include custom types at compile-time (e.g. in the JAR) for use in UDF declarations
* Ability to list all custom types

Schema Registry integration is not considered within scope.

## Value/Return

Real data is complex, nested and referential. This feature will help us dramatically improve the
user interaction with complicated schemas. 

## Public APIS

### CREATE TYPE

```sql
CREATE TYPE <type_name> AS <type>;
```

The `CREATE TYPE` syntax will allow KSQL users to register a type alias directly in SQL (either 
interactive or headless modes). Any types registered using this command can be leveraged in any
future statement. 

Any attempts to register the same type twice without a corresponding `DROP TYPE` will fail.

### DROP TYPE 

```sql
DROP TYPE <type_name>;
```

The `DROP TYPE` syntax will allow KSQL users to remove a type alias from KSQL.

### SHOW TYPES

```sql
SHOW TYPES;
```

The `SHOW TYPES` command will list all custom types and their type definitions. A sample output
for this command would be:
```
|---------------|--------------------------------------------------------------|------------------|
|     name      |       definition                                             | source           |
|---------------|--------------------------------------------------------------|------------------|
| ADDRESS       | STRUCT<number INTEGER, street VARCHAR, city VARCHAR>         | types.sql        |
| PERSON        | STRUCT<firstname VARCHAR, lastname VARCHAR, address ADDRESS> | types.sql        |
| COMPANY       | STRUCT<name VARCHAR, headquarters ADDRESS>                   | CLI              |
|---------------|--------------------------------------------------------------|------------------|
```

### Extension Directory

The `ksql.extension.dir` will now also recognize `.sql` files that contain only `CREATE TYPE`
commands. These commands will all be run before compiling any UDFs so these custom type declarations
can be used in the `@Udf` annotations. They will be loaded in natural order to ensure that the
loading behavior is deterministic (using `Comparator#naturalOrder` and `String#CompareTo`).

## Design

There will be a rewriter phase that will replace any aliased type with the full schema. For UDFs, 
the annotation will lookup the schema directly and compile the UDF using the full schema. There
are no restrictions on what types can be added as custom types, and custom types can be composite.

This rewriting will be done _before_ enqueuing the command on the command topic to make sure that
if the types change the existing statements will already be resolved. The user will see a success
message that contains the rewritten schema.

If a type is already registered via CLI, but is then added to the extensions directory, the value
in the directory will take precedence over the CLI registered value. The CLI will reject calls to
register types that are already registered in the extensions directory.

## Future Work
* in v1, `DESCRIBE`/`Explian` commands would show the flattened types (resolved) - it is better to 
keep the original type structure and map it back for `DESCRIBE`/`Explain` commands
* SchemaRegistry integration is not in scope

## Test plan

Nothing special of note here.

## Documentation Updates

* The `syntax-reference.rst` will have an updated section on `CREATE TYPE`:

>**Synopsis**
>
>.. code:: sql
>
>    CREATE TYPE <type_name> AS <type>;
>
> Example:
>
>.. code:: sql
>
>   CREATE TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
>   CREATE TYPE PERSON AS STRUCT<firstname VARCHAR, lastname VARCHAR, address ADDRESS>;
>   CREATE TYPE COMPANY AS STRUCT<name VARCHAR, headquarters ADDRESS>;
>
>**Description**
>
>Register the ``<type>`` under the alias ``<type_name>`` to be used in future statements. Any valid
>column schema can be registered as a custom type, and the type_name must be exclusively letters
>```[A-Z]``` and the underscore `_` character.

* The `syntax-reference.rst` will have an updated section on `DROP TYPE`:

>**Synopsis**
>
>.. code:: sql
>
>    DROP TYPE <type_name>;
>
>**Description**
>
> Removes the association between ``<type_name>`` and any currently registered type. This command
> will fail if any UDFs are compiled using this type name or any currently running queries leverage
> this alias.

* The `config-reference.rst` will include a new section on `ksql.extension.dir`:

> The extension dir contains two entities: JARs that contain UDFs/UDAFs and `.sql` files that
> contain any custom types. The JARs are described more in :ref:`deploying-udf`. Any `.sql` files
> must contain only statements of ``CREATE TYPE``.

# Compatibility Implications

N/A

## Performance Implications

N/A

## Security Implications

N/A

## Rejected Alternatives

### Syntax

Instead of the original `REGISTER TYPE` syntax, the suggestion is to follow what Postgres-SQL does
and introduce `CREATE TYPE`/`DROP TYPE` (https://www.postgresql.org/docs/9.5/sql-createtype.html)

### Annotations

We could implement the types as annotations instead of allowing users to submit a `.sql` file that
contains the `CREATE TYPE` statements. For example:
```java
@TypeRegistry
public class MyTypes {
  @CustomType("ADDRESS")
  public final static String ADDRESS = "STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;";
  
  @CustomType("PERSON")
  public final static String PERSON = "STRUCT<firstname VARCHAR, lastname VARCHAR, addr ADDRESS>;";
}
```

This could have the benefits of allowing users to specify the types as `Schema` objects instead of
Strings. This is not ideal, however, because reflection can be fragile and the annotations don't
feel natural. Furthermore, this would require maintaining two separate ways of registering custom 
types.
