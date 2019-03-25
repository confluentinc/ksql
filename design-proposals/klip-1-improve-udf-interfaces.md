# KLIP-1: Improve UDF Interfaces

**Author**: agavra | 
**Release Target**: 5.3 | 
**Status**: In Discussion | 
**Discussion**: link

**tl;dr:** *Address the most common feature requests related to UDF/UDAFs including
struct support, generic types, variable arguments and complex aggregation*


## Motivation and background

There have been many requests ([#2163](https://github.com/confluentinc/ksql/issues/2163)) to
improve the UDF/UDAF interface in KSQL. Of these features, four stand out:

- _Custom Struct Support_ - UDFs can only operate on standard Java types (`Map`, `List`, `String`, 
etc...) and do not accept structured types. This dramatically limits what KSQL can handle, as
much of the data flowing through kafka is structured Avro/JSON.
- _Generics Type Support_ - Today, there is no way to implement a single UDF that supports data 
structures with implicit types. For example, implementing a generic `list_union` operator would
require different method signatures for each supported type. This quickly becomes unmanageable
when there are multiple arguments.
- _Variable Argument Support_ - Varargs are required to create UDFs that accept an arbitrary
number of inputs. Creating a simple `concat` UDF that could sum multiple columns would require
creating method signatures for as many input combinations as desired.
- _Complex Aggregation Models_ - Today, KSQL only supports tracking a single variable for 
aggregation and it must mirror the output type. To implement an `avg` UDAF, it is necessary to store 
both a `count` and a `running_total`.

## Scope

All changes will be made for the new annotation-based UDF interfaces and will not be backported to
the legacy implementation. The bullet points mentioned above are all in scope, but the following 
will not be included in this KLIP:

- UDTFs - multiple output operations are more complicated and will require a KLIP of their own
- Generics/Varargs for UDAFs - the support for Generics/Varargs will only extend to UDFs.

## Value

Basic UDF operations that are supported in SQL languages will now be unblocked for KSQL. Some of
these are outlined below:

|      Improvement      |                                 Value                                  |
|:---------------------:|------------------------------------------------------------------------|
| Generics support      | Unblock `ARRAY_LENGTH`, `ARRAY_SLICE`, `ARRAY_TO_STRING`, etc...       |
| Varargs support       | `CONCAT`, `MAX`, `MIN` etc...                                          |
| Complex Aggregation   | `AVG`, `PERCENTILE`, etc...                                            |
| Structured UDFs       | Arbitrary data handling without intermediary transforms                |

## Public APIS

Some public APIs will be changed, though all will be changed in a data backwards compatible fashion:

|      Improvement      |                               API change                               |
|:---------------------:|------------------------------------------------------------------------|
| Generics support      | UDF interface will accept inferred generics (e.g. `List<T>`)           |
| Varargs support       | UDF interface will accept `...` in the method declarations             |
| Complex Aggregation   | UDAFs may include `VR export(A agg)` to convert aggregated to output   |
| Structured UDFs       | UDF/UDAF interfaces will accept the Kafka Connect `Struct` type        |

## Design

Each of these improvements is relatively self-contained and can be worked on independently and in
parallel. Below are detailed designs for each:

### Structured UDFs

After refactoring done in [#2411](https://github.com/confluentinc/ksql/pull/2411), the type coercion 
ensures that the validation does not unnecessarily prevent usage of `Struct`. To complete support
for sturcutred UDFS, we also need to have some mechanism to create the output schema. To address 
this, we can resolve the schema as part of the UDF specification, requiring users to specify the 
struct schema inline.

```java
@UdfSchema
public static final Schema SCHEMA = SchemaBuilder.struct()...build();

@Udf(schema="STRUCT<'VAL' VARCHAR, 'LENGTH' INT>")
public Struct generate(
    @UdfParam(schema="STRUCT<'VAL' VARCHAR>") final Struct from
  ) {
  return new Struct(...);
}
```

`UdfFactory` could then load the correct UDF given the schema by matching the schema of the type 
against various different methods. It will be possible to support multiple methods with different
struct schemas if the name of the udf method is changed (not that the java method name is ignored
from actual evaluation).

### Generics

Today, we eagerly convert UDF output types to corresponding Connect schema types at the time that we
compile the UDFs and not the time that we use them in a `SELECT` clause. Since there is no 
corresponding "unresolved" type in Connect, we fail this conversion. To solve this problem, we can
infer the type from the source schema and compile the code to work with any component type (e.g.
omit the generics from the compiled code).

```java
@Udf("returns a sublist of 'list'")
public <T> List<T> sublist(
    @UdfParameter final List<T> list, 
    @UdfParameter final int start, 
    @UdfParameter final int end) {
  return list.subList(start, end);
}
```

We need to ensure that the output type can be inferred. This means that we need to fail if the 
signature is something like `<T> List<T> convert(List<String> list)`. This can be done during 
compilation by following a simple rule: any generic type used in the output must be present in at
least one of the parameters. We can access this information via reflection.

The `DESCRIBE FUNCTION` call to describe such generic values will display a SQL type that is not
supported in DDL statements (e.g. `ARRAY<ANY<T>>`) (See documentation section).

**NOTE:** Supporting a wildcard output type is not covered by this design since we would not be able 
to generate the output schema for select statements, however supporting wildcards in the input types 
(e.g. `Long length(List<?> list)`) should be possible.

### Varargs

Varargs boils down to supporting native Java arrays as parameters and ensuring that component types 
are resolved properly. Anytime a method is registered with variable args (e.g. `String... args`) we 
will register a corresponding function with the wrapping array type (e.g. `String[]`). 

We will need to resolve methods such as `foo(Int, String...)` and `foo(String, String...)` as well 
as accept empty arguments to `foo(String...)`. If any parameter is `null`, it will be considered 
valid for any vararg declaration.

```java
@Udf("sums all arguments in 'args'")
public int sum(@UdfParameter final int... args) {
  return Arrays.stream(args).sum();
}
```

### Complex Aggregation

Complex aggregation for UDAFs requires the ability to aggregate on a value that is later reduced to
a value of a different type. To support this, we will change the `Udaf` interface in a backwards
incompatible way (code built using the old `Udaf` interface will not compile, but migration is
straightforward):
```java
/**
* ...
* @param <V> value type (the value that is accepted)
* @param <A> aggregate type
* @param <VR> reduce type (the value to return)
*/
public interface Udaf<V, A, VR> {
  
  ...
 
  /**
   * Redcues an aggregate into the return type.
   * 
   * @param aggregate the running aggregate
   * @return the final return value
   */ 
  VR reduce(A aggregate);
  
}
```

`reduce` will be taken as the behavioral parameter to a `mapValues` task that will be applied after
the aggregate task in the generated KStreams topology.

## Test plan

Each of these improvements will come with some built-in UDF/UDAF examples. These examples will then
be comprehensively covered by both unit tests as well as corresponding Query Translation Tests.
Specifically, the UDFs described in the [Value](#value) section will all be implemented alongside
this KLIP. For the new UDAF complex aggregation, we will ensure that the generated topologies do not
include any unexpected steps such as a repartition.

## Documentation Updates

### Varargs

* The `Example UDF Class` in `udf.rst` will be extended to showcase the new features. Namely, we
will replace the multiply double method to include varargs:

>```java
>      @Udf(description = "multiply N non-nullable DOUBLEs.")
>      public double multiply(final double... values) {
>        return Arrays.stream(values).reduce((a, b) -> a * b);;
>      }
>```

* The `DESCRIBE FUNCTION` for vararg functions will need to be updated to use the `...` syntax:
> ```
> Name        : ARRAYCONTAINS
> Author      : Confluent
> Type        : scalar
> Jar         : internal
> Variations  :
> 
> 	Variation   : ARRAYCONTAINS(VARCHAR... )
> 	Returns     : BOOLEAN
> ```

### Struct Support for UDF

* The `Supported Types` section in `udf.rst` will include `Struct` and have an updated note at the 
bottom which reads:

> Note: Complex types other than List, Map and Struct are not currently supported. Generic types
> for List and Map are supported, but component elements must be of one of the supported types. If
> the types are generic, the output type must be able to be inferred from one or more of the input
> parameters.

* Function descriptions as part of `DESCRIBE FUNCTION` should also show the schema for any `Struct`
that is used:

> ```
> Name        : FOO
> Author      : Confluent
> Type        : scalar
> Jar         : internal
> Variations  :
>
>       Variation   : FOO(STRUCT<'VAL' BIGINT, 'OTHER' VARCHAR>)
>       Returns     : STRUCT<'VAL' BIGINT, 'ANOTHER' LIST<VARCHAR>)
> ```

### Generics

* The `DESCRIBE FUNCTION` should indicate when a function accepts generics using `ANY<?>` where `?`
is a letter that begins with `T` and is incremented for each inferred type.

> ```
> Name        : FOO
> Author      : Confluent
> Type        : scalar
> Jar         : internal
> Variations  :
>
>       Variation   : FOO(ARRAY<ANY<T>>, ANY<U>)
>       Returns     : ANY<T>
> ```

### Complex Aggregation

* The `UDAF` section in `udf.rst` will update the example from `Sum` to `Average` and include
non-trivial usage of the `reduce` API:

> The class below creates a UDAF named ``my_average``...
>
> .. code:: java
> 
>```java
>@UdafDescription(name="my_average", description="averages")
>public class AverageUdaf {
>  
>  private static final Schema SCHEMA = SchemaBuilder.struct()
>                                         .field("sum", Schema.INT64_SCHEMA)
>                                         .field("count", Schema.INT32_SCHEMA)
>                                         .build();
>
>  @UdafFactory(description = "averages longs")
>  public static TableUdaf<Long, Struct, Double> createAverageLong() {
>    return new TableUdaf<Long, Struct, Double>() {
>      @Override
>      public Struct initialize() { 
>        return new Struct(SCHEMA).put("sum", 0L).put("count", 0); 
>      }
>      
>      @Override
>      public Struct aggregate(final Long value, final Struct aggregate) {
>        return new Struct(SCHEMA)
>                  .put("sum", aggregate.get("sum") + value)
>                  .put("count", aggregate.get("count") + 1);
>      }
>      
>      @Override
>      public Struct undo(final Long valueToUndo, final Struct aggregate) {
>        return new Struct(SCHEMA)
>                  .put("sum", aggregate.get("sum") - valueToUndo)
>                  .put("count", aggregate.get("count") - 1);
>      }
>      
>      @Override
>      public Struct merge(final Struct aggOne, final Struct aggTwo) {
>        return new Struct(SCHEMA)
>                  .put("sum", aggOne.get("sum") + aggTwo.get("sum"))
>                  .put("count", aggOne.get("count") + aggTwo.get("count"));
>      }
>      
>      @Override
>      public Double reduce(final Struct aggregate) {
>        return ((Double) aggregate.getInt64("sum")) / aggregate.getInt32("count");
>      }
>    };
>  }
>}
>```

* For exportable UDAFs, we will also need to generate updated documentation for `DESCRIBE FUNCTION`
calls to make sure that the output value is not the intermediate value, but rather the final 
exported value. For example:

> ```
> ksql> DESCRIBE FUNCTION AVERAGE;
> 
> Name        : AVERAGE
> Author      : Confluent
> Overview    : Returns the average value for a window.
> Type        : aggregate
> Jar         : internal
> Variations  :
> 
>	Variation   : AVERAGE(BIGINT)
>	Returns     : DOUBLE
> // Note that the signature for this is actually Udaf<Long, Struct, DOUBLE> as seen above
>```

* Syntax reference needs to be updated to reflect any new UDF/UDAFs that are implemented as part of
this KLIP.

# Compatibility implications

N/A

## Performance implications

N/A
