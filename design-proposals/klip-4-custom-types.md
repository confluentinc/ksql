  # KLIP-4: Custom Type Registry

**Author**: agavra | 
**Release Target**: 5.4 | 
**Status**: In Discussion | 
**Discussion**: https://github.com/confluentinc/ksql/pull/2894

**tl;dr:** *Introduce a feature that makes custom types easier to work with in KSQL by aliasing
complex type declarations.*

## Motivation and background

Kafka often contains very complex nested data structures and it is common to see schemas nested with
other schemas. It is desirable to be able to define this custom schema in KSQL once and reference it
in all future statements. For example, imagine the following schema declarations:

```sql
REGISTER TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
REGISTER TYPE PERSON AS STRUCT<firstname VARCHAR, lastname VARCHAR, address ADDRESS>;
REGISTER TYPE COMPANY AS STRUCT<name VARCHAR, headquarters ADDRESS>;
```

With this feature organizations can leverage existing schemas in their KSQL queries and share 
data with other parts of their organizations.

## Scope

* Syntax to register and unregister custom type aliases
* Ability to include custom types at compile-time (e.g. in the JAR) for use in UDF declarations

## Value/Return

Real data is complex, nested and referential. This feature will help us dramatically improve the
user interaction with complicated schemas. 

## Public APIS

### REGISTER TYPE

```sql
REGISTER TYPE <type_name> AS <type>;
```

The `REGISTER TYPE` syntax will allow KSQL users to register a type alias directly in SQL (either 
interactive or headless modes). Any types registered using this command can be leveraged in any
future statement. 

Any attempts to register the same type twice without a corresponding `UNREGISTER TYPE` will fail.

### UNREGISTER TYPE

```sql
UNREGISTER TYPE <type_name>;
```

The `UNREGISTER TYPE` syntax will allow KSQL users to remove a type alias from KSQL. This statement
will fail if the type is being used in any active query or UDF.

### Extension Directory

The `ksql.extension.dir` will now also recognize `.sql` files that contain only `REGISTER TYPE`
commands. These commands will all be run before compiling any UDFs so these custom type declarations
can be used in the `@Udf` annotations.

## Design

There will be a rewriter phase during parsing that will replace any aliased type with the full
schema. For UDFs, the annotation will lookup the schema directly and compile the UDF using the 
full schema.

**NOTE:** the UI should be aware of this feature, and it would be nice-to-have a list of all 
existing registered custom types.

## Test plan

Nothing special of note here.

## Documentation Updates

* The `syntax-reference.rst` will have an updated section on `REGISTER TYPE`:

>**Synopsis**
>
>.. code:: sql
>
>    REGISTER TYPE <type_name> AS <type>;
>
> Example:
>
>.. code:: sql
>
>   REGISTER TYPE ADDRESS AS STRUCT<number INTEGER, street VARCHAR, city VARCHAR>;
>   REGISTER TYPE PERSON AS STRUCT<firstname VARCHAR, lastname VARCHAR, address ADDRESS>;
>   REGISTER TYPE COMPANY AS STRUCT<name VARCHAR, headquarters ADDRESS>;
>
>**Description**
>
>Register the ``<type>`` under the alias ``<type_name>`` to be used in future statements. Any valid
>column schema can be registered as a custom type, and the type_name must be exclusively letters
>```[A-Z]```.

* The `syntax-reference.rst` will have an updated section on `UNREGISTER TYPE`:

>**Synopsis**
>
>.. code:: sql
>
>    UNREGISTER TYPE <type_name>;
>
>**Description**
>
> Removes the association between ``<type_name>`` and any currently registered type. This command
> will fail if any UDFs are compiled using this type name or any currently running queries leverage
> this alias.

* The `config-reference.rst` will include a new section on `ksql.extension.dir`:

> The extension dir contains two entities: JARs that contain UDFs/UDAFs and `.sql` files that
> contain any custom types. The JARs are described more in :ref:`deploying-udf`. Any `.sql` files
> must contain only statements of ``REGISTER TYPE``.

# Compatibility Implications

N/A

## Performance Implications

N/A

## Security Implications

N/A

## Rejected Alternatives

### Annotations

We could implement the types as annotations instead of allowing users to submit a `.sql` file that
contains the `REGISTER TYPE` statements. For example:
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
