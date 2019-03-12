# KLIP 2 - Support inline, multilingual UDFs

**Author**: [Mitch Seymour][mitch-seymour]

**Release target**: 5.3

**Status**: In Discussion

<!-- TODO: replace with link to PR -->
**Discussion**: [link][design_pr]

[design_pr]: https://github.com/confluentinc/ksql/pull/2553
[mitch-seymour]: https://github.com/mitch-seymour

## tl;dr

Allowing UDFs to be written in languages other than Java (e.g. JavaScript, Python, Ruby) will:
- Increase UDF-related feature adoption among non-Java developers
- Reduce the amount of code and time that is needed to deploy simple UDFs (by forgoing the relatively tedious build / deployment process of Java-based UDFs)
- Enable rapid prototyping of new data transformation logic
- Strengthen feature parity between KSQL and certain RDMBSes (e.g. Postgres, which supports [inline Python UDFs][postgres])

[postgres]: https://www.postgresql.org/docs/current/plpython-funcs.html

## Motivation and background

KSQL allows developers to leverage the power of Kafka Streams without knowing Java. However, non-Java developers who use KSQL may quickly find themselves locked out of one of the most powerful features of KSQL: the ability to write custom functions for processing data. Multilingual UDFs will give these developers an opportunity to create their own UDFs using a language they may already be familiar with.

Furthermore, even Java developers may find the process of writing a simple UDF a little tedious. Java-based UDFs require some level of ceremony to build and deploy. To illustrate this point, consider the following example. Let's create a UDF named `MULTIPLY` that multiplies 2 numbers.

__Current approach:__

- Write a Java class that implements the business logic of our UDF (multiply two numbers)
- Add the `@UdfDescription` and `@Udf` annotations to our class
- Create a build file (e.g. `pom.xml` or `build.gradle`) for building our project
- Package the artifact into an uber JAR
- Copy the uber JAR to the `ext/` directory (`ksql.extension.dir`)
- Restart KSQL server instances to pick up the new UDF

__New approach:__

As you can see above, implementing the business logic of our UDF is only the first of many steps using the current approach. For simple UDFs (i.e. UDFs that don't require third-party dependencies, and can be expressed in a few lines of code), it would be much easier if we could just worry about implementation details of our function, and not the build / deployment process as well. For example, using the new `CREATE OR REPLACE` query, we could create the `MULTIPLY` UDF as follows:

```sql
CREATE OR REPLACE FUNCTION MULTIPLY(x INT, y INT) 
RETURNS INT
LANGUAGE JAVASCRIPT
AS $$
  (x, y) => x * y
$$ 
WITH (author='Mitch Seymour', description='multiply two numbers', version='0.1.0');
```

The above query would automatically update the internal function registry as needed so that we can avoid restarting any KSQL servers to pick up the new UDF. It also requires a lot less code than the Java equivalent. Less code, combined with a quicker deployment model, means a better development experience for UDF creators.

## What is in scope

- Extending the KSQL language to support inline, multilingual UDFs
- Quick deployment process for multilingual UDFs, with support for hot-reloading of UDFs that are created via the new `CREATE OR REPLACE` query
- A new boolean KSQL configuration parameter: `ksql.experimental.features.enabled`. Defaults to `false`

## What is not in scope

- UDAFs
- Non-Java UDFs that cannot be expressed as an inline function (e.g. a larger program that would need to be loaded from disk)

The above items are too complicated for the initial implementation of this feature. We should start simple and build a solid foundation with the current proposal before attempting more complicated variations of this work.

## Value/Return

Allowing users to write UDFs in languages other than Java will provide the following benefits:

- Unlock UDFs for non-Java developers, which will likely increase UDF-related feature adoption
- Write less code for implementing simple, custom functions.
    - Java-based UDFs generally require a project structure, build process, annotation requirements, dropping a JAR in pre-defined location and restarting the KSQL server.
    - Inline multilingual UDFs only require the business logic of the function itself
- A streamlined deployment process for non-Java UDFs with support for hot-reloading (instead of requiring users to restart KSQL server instances)
- Enable rapid prototyping of data transformation logic

## Public APIS

### KSQL language

The KSQL language will be extended with the following queries:

1. Query for creating inline, multilingual UDFs:

```sql
CREATE (OR REPLACE?) FUNCTION function_name ( { field_name data_type } [, ...] )
RETURNS data_type
LANGUAGE language_name
AS $$
  inline_script
$$
WITH ( property_name = expression [, ...] );
```

Example:

Here's an example UDF that coverts HTTP response codes (500, 404, [418][i_am_a_teapot]), to a status major string (`5xx`, `4xx`, etc).

[i_am_a_teapot]: https://httpstatuses.com/418

```sql
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
  (code) => code.toString().charAt(0) + 'xx'
$$
WITH (author='Mitch Seymour', description='js udf example', version='0.1.0');
```

Functions created using this new query will be discoverable via `SHOW FUNCTIONS` and can be described using the `DESCRIBE FUNCTION` query. We may want to impose a max identifier length for the function name (e.g. 60 characters).

2. Query for dropping UDFs that were created using the method above.

```sql
DROP FUNCTION function_name
```

If a user tries to drop an internal or Java-based UDF, they will receive an error.

### Configurations
- A new configuration to enable experimental features. The reason this should be considered experimental is because our solution relies on GraalVM (see [Design](#design)), which is awaiting a 1.0 release (release candidate versions are available and have worked well in [prototypes of multilingual UDFs in KSQL][prototypes]).

[prototypes]: https://github.com/magicalpipelines/docker-ksql-multilingual-udfs-poc

## Design

[GraalVM Community Edition][gce] is a virtual machine that supports polyglot programming. It is a drop-in replacement for Java 8 and soon to be Java 11. Users who would like to write UDFs in languages other than Java will be expected to run KSQL on GraalVM (instead of the HotSpot VM). Users will also have control over which languages they can use in multilingual UDFs by installing the appropriate components using the [Graal updater][gu], which is included in the GraalVM installation. For example. to install `python`, one could simply run:

```bash
$ gu install python
```

As new languages become available, they will appear in the GraalVM component catalog and can be used immediately without requiring updates to the KSQL codebase. Here's an example of how to view the GraalVM component catalog:

```bash
$ gu available

ComponentId              Version             Component name
----------------------------------------------------------------
python                   1.0.0-rc12          Graal.Python
R                        1.0.0-rc12          FastR
ruby                     1.0.0-rc12          TruffleRuby
```

__Note:__ JavaScript is also included in the GraalVM installation, and does not need to be installed using `gu`.


Regardless of which VM users run KSQL on, the GraalVM SDK will be added as a dependency to the `ksql-engine` and `ksql-parser` subprojects. When a user invokes the `CREATE OR REPLACE FUNCTION` query, the SDK will be used to create a [polyglot context][pg] that will determine whether or not the provided function is executable. For example, if a user were to invoke the following query while running KSQL on the HotSpot VM:

```
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
  (code) => code.toString().charAt(0) + 'xx'
$$ 
```

They will receive an error along the lines of:

```
Function is not executable. GraalVM is required for multilingual UDFs
```

Similarly, if KSQL is running on GraalVM but the inline script is not a valid lambda / executable, e.g.

```
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
  "hello"
$$ 
```

Then an error will be returned:

```
Function is not executable. Inline scripts must be defined as a lambda and must not contain syntax errors.
```

The GraalVM SDK handles this kind of validation for us, we just need to simply leverage the appropriate methods (e.g. [Value::canExecute][can_execute]

[can_execute]: https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/Value.html#canExecute--
[gce]: graalvm.org
[gu]: https://www.graalvm.org/docs/reference-manual/graal-updater/
[pg]: https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/Context.html

In order to support the hot-reloading feature, we will need to add a new method to the [MutableFunctionRegistry][mutable_fn_registry]: `addOrReplace`. This will only be invoked when leveraging the new `CREATE OR REPLACE` query. 

[mutable_fn_registry]: https://github.com/confluentinc/ksql/blob/5.2.x/ksql-common/src/main/java/io/confluent/ksql/function/MutableFunctionRegistry.java

Furthermore, a `dropInlineFunction` method will beed to be added to the [MutableFunctionRegistry][mutable_fn_registry] in order to drop any function that was created via the new `CREATE OR REPLACE` query. This method will be invoked whenever a `DROP FUNCTION` query is executed.

Finally, Nashorn was also considered for implementing non-Java based UDFs, but [has officially been deprecated][nashorn_deprecated].

[nashorn_deprecated]: http://openjdk.java.net/jeps/335

## Test plan

Tests will cover the following:

- Query parsing / translation
- New methods added to the internal function registry for updating / dropping scripted UDFs via `CREATE OR REPLACE FUNCTION` and `DROP FUNCTION` queries
- Any other changes that are made to existing classes
- Mocked failure scenarios will include scripts that aren't executable (e.g. because of syntax errors, guest language isn't installed, etc), and `ClassCastException` / `NullPointerException` when the script doesn't return the expected value


## Documentation Updates
- The `Implement a Custom Function` section will need to be updated in the [KSQL Function Reference](/docs/developer-guide/udf.rst) to include instructions for implementing non-Java UDFs. Any reference to `UDFs` in the current documentation will be updated to `Java UDFs`.

- The [Syntax Reference](/docs/developer-guide/syntax-reference.rst) will need to be updated to include the new commands:

> CREATE FUNCTION
> ---------------
> 
> **Synopsis**
> 
> ```sql
> CREATE (OR REPLACE?) FUNCTION function_name ( { field_name data_type } [, ...] )
> RETURNS data_type
> LANGUAGE language_name
> AS $$
>   inline_script
> $$
> WITH ( property_name = expression [, ...] );
> ```
> 
> **Description**
> 
> Create a new function with the specified arguments and properties. __Note:__ this is an experimental feature, so the following requirements must be met before running a `CREATE FUNCTION` query.
> 
> - `ksql.experimental.features.enabled` is set to `true`
> - You are running KSQL on GraalVM
> - If language you specify in the `CREATE FUNCTION` query isn't bundled in GraalVM, you must install the appropriate component using the [Graal Updater][graal-updated] (`gu`).
> - The function must be executable. In other words, your function should be defined as a lambda / anonymous function in the language you choose.
> 
> [graal-updater]: https://www.graalvm.org/docs/reference-manual/graal-updater/#component-installation
> 
> The supported field and return data types are:
> 
> -  ``BOOLEAN``
> -  ``INTEGER``
> -  ``BIGINT``
> -  ``DOUBLE``
> -  ``VARCHAR`` (or ``STRING``)
> -  ``ARRAY<ArrayType>`` (JSON and AVRO only. Index starts from 0)
> -  ``MAP<VARCHAR, ValueType>`` (JSON and AVRO only)
> -  ``STRUCT<FieldName FieldType, ...> (JSON and AVRO only)``
> 
> The WITH clause supports the following properties:
> 
> | Property                | Description                                                                                |
> |-------------------------|--------------------------------------------------------------------------------------------|
> | AUTHOR                  | The author of the function                                                                 |
> | DESCRIPTION             | A description of the function                                                              |
> | VERSION                 | The version (e.g. `0.1.0`) of the function                                                 |
> 
> Example:
> 
> ```sql
> 
> CREATE OR REPLACE FUNCTION MULTIPLY(x INT, y INT)
> RETURNS INT
> LANGUAGE PYTHON
> AS $$
>   lambda x, y: x * y
> $$
> WITH (author='Your name', description='multiply two numbers', version='0.1.0');
> ```
  
- The [KSQL Configuration Parameter Reference](/docs/installation/server-config/config-reference.rst) will need to be updated to include the new configuration parameter: `ksql.experimental.features.enabled`

   > ----------------------------------
   > ksql.experimental.features.enabled
   > ----------------------------------
   > Indicates whether or not experimental features are enabled. Defaults to `false`. Setting this to `true` will enable the following experimental features:
   > - Multilingual UDFs

# Compatibility implications

- Users running KSQL on the HotSpot VM will _not_ be impacted by the feature at all. Using the new queries will result in errors indicating that the user must run KSQL on GraalVM if they wish to implement a non-Java UDF. However, all other queries will continue to work as expected.
- Users running KSQL on GraalVM will be able to execute the new queries. Existing queries and functionality will still be available, as well.

## Performance implications

The code changes themselves do not have any performance implications. However, running KSQL on top of GraalVM may offer some performance advantages as mentioned [here][graalvm-perf].

[graalvm-perf]: https://www.graalvm.org/docs/why-graal/#for-java-programs
