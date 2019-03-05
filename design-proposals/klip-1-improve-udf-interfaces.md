# KLIP-1: Improve UDF Interfaces

**Author**: agavra | 
**Release Target**: 5.3 | 
**Status**: In Discussion | 
**Discussion**: link

**tl;dr:** *Address the most common feature requests related to UDF/UDAFs including
struct support, wildcard types, variable arguments and complex aggregation*


## Motivation and background

There have been many requests ([#2163](https://github.com/confluentinc/ksql/issues/2163)) to
improve the UDF/UDAF interface in KSQL. Of these features, four stand out:

- _Custom Struct Support_ - UDFs can only operate on standard Java types (`Map`, `List`, `String`, 
etc...) and do not accept structured types. This dramatically limits what KSQL can handle, as
much of the data flowing through kafka is structured Avro/JSON.
- _Wildcard Type Support_ - Today, there is no way to implement a single UDF that supports data 
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
- Wildcards/Varargs for UDAFs - the support for Wildcards/Varargs will only extend to UDFs.

## Value

Basic UDF operations that are supported in SQL languages will now be unblocked for KSQL. Some of
these are outlined below:

|      Improvement      |                                 Value                                  |
|:---------------------:|------------------------------------------------------------------------|
| Wildcard support      | Unblock `ARRAY_LENGTH`, `ARRAY_SLICE`, `ARRAY_TO_STRING`, etc...       |
| Varargs support       | `CONCAT`, `MAX`, `MIN` etc...                                          |
| Complex Aggregation   | `AVG`, `PERCENTILE`, etc...                                            |
| Structured UDFs       | Arbitrary data handling without intermediary transforms                |

## Public APIS

Some public APIs will be changed, though all will be changed in a backwards compatible fashion:

|      Improvement      |                               API change                               |
|:---------------------:|------------------------------------------------------------------------|
| Wildcard support      | UDF interface will accept wildcards and unspecified generics           |
| Varargs support       | UDF interface will accept `...` in the method declarations             |
| Complex Aggregation   | UDAFs may include `VR export(A agg)` to convert aggregated to output   |
| Structured UDFs       | UDF/UDAF interfaces will accept the Kafka Connect `Struct` type        |

## Design

Each of these improvements is relatively self-contained and can be worked on independently and in
parallel. Below are detailed designs for each:

### Structured UDFs

After refactoring done in [#2411](https://github.com/confluentinc/ksql/pull/2411), the type coercion 
is already in place to support `Struct` types as input parameters. Minor changes need to be made to 
ensure that the validation does not unnecessarily prevent usage of `Struct`.

A noteworthy extension that may be worth supporting is dedicated AVRO types in the signatures for 
UDFs, but this is not covered in this KLIP.

```java
@Udf("Checks if the employee has a valid record (i.e. contains a valid name and email)")
public boolean isValid(
    @UdfParameter final Struct employee) {
  return employee.get("firstname").matches("[A-Z][a-z]*")
    && employee.get("email").endsWith("@company.io");
}
```

There is more work to be done in order to support `Struct` as the return value of UDFs, namely we
must have some mechanism to create the output schema. There are two ways to resolve the output
schema:

* Resolve the schema at runtime, inferring it from the output object. This can work, but runs the
risk that the UDF is inconsistent (e.g. returns two structs with different schemas).
* Resolve the schema as part of the UDF specification. This adds more structure and predictability,
but may become tricky to evolve and we need a good API to do this, especially if it is necessary to
specify complicated nested structs. Below are three candidate ways to specify the schema:

```java
@UdfSchema
public static final Schema SCHEMA = SchemaBuilder.struct()...build();

@Udf("Checks if the employee has a valid record (i.e. contains a valid name and email)")
@UdfReturn(value  = "STRUCT<'A' INT, 'B' VARCHAR>") // sample specification annotation
@UdfReturn(file   = 'schema_def.kschema')           // another way pointing to a file
@UdfReturn(schema = "name.space.MyClass.SCHEMA")    // another way that would resolve a java object
public Struct generate() {
  return new Struct(...);
}
```

### Wildcards 

Today, we eagerly convert UDF output types to corresponding Connect schema types at the time that we
compile the UDFs and not the time that we execute them. Since there is no corresponding "wildcard" 
type in Connect, we fail this conversion. There is no such limitation on the code generation or the 
input parameters to UDFs.

Solving this problem can be done in two ways:

- We can infer the types from the kafka topic schema or the metadata schema. This may not be
possible for JSON or DELIMITED topic schemas.
- We can resolve the schema at runtime. This is simple and only takes into account the data being
deserialized, but may have performance implications. To address this concern, we could cache the
schema after the first deserialization.

```java
@Udf("returns a sublist of 'list'")
public List sublist(
    @UdfParameter final List list, 
    @UdfParameter final int start, 
    @UdfParameter final int end) {
  return list.subList(start, end);
}
```

**NOTE:** Explicitly typing with a wildcard (e.g. `<?>`) is not covered by this design. One way to
allow for it is to simply scrub the wildcard if added, treating the type as if the wildcard did not
exist.

### Varargs

Varargs boils down to supporting native Java arrays as parameters and ensuring that component types 
are resolved properly. Anytime a method is registered with variable args (e.g. `String... args`) we 
will register a corresponding function with the wrapping array type (e.g. `String[]`). At runtime, 
if no function matches the explicit parameter types, we will fallback to search for any array-type 
parameter method declaration of the same type iff all parameters are of a single type. If any
parameter is null, it will be considered valid for any vararg declaration.

This proposal does not cover supporting `Object` as a parameter to a method in order to allow for 
"generic variable argument" UDFs, but supporting it can be an extension.

```java
@Udf("returns a sublist of 'list' t")
public int sum(@UdfParameter final int... args) {
  return Arrays.stream(args).sum();
}
```

### Complex Aggregation

We will allow users to supply an additional method `VR export(A agg)`. This method will be taken
as the behavioral parameter to a `mapValues` task that will be applied after the `aggregate` task 
in the generated KStreams topology. To support this in a backwards compatible fashion, we will 
introduce a new interface `Exportable`, that UDFs may implement. Only UDAFs that implement this 
interface will have the `mapValues` stage applied to them.

```java
class AverageUdaf implements Udaf<Long, Struct>, Exportable<Struct, Long> {
  
  private static final Schema SUM_SCHEMA = 
      SchemaBuilder.struct()
                  .field("sum", Schema.INT64_SCHEMA)
                  .field("count", Schema.INT64_SCHEMA).build();
  
  @Override
  public Struct initialize() {
    return new Struct(SCHEMA);
  }
  
  @Override
  public Struct aggregate(final Long val, final Struct agg) {
    agg.put("sum", agg.get("sum") + val);
    agg.put("count", agg.get("count") + 1);
    return agg;
  }
  
  @Override
  public Struct merge(final Struct agg1, final Struct agg2) {
    agg1.put("sum", agg1.get("sum") + agg2.get("sum"));
    agg1.put("count", agg1.get("count") + agg2.get("count"));
    return agg1;
  }
 
  /**
  * This is the new method that overrides {@code Exportable#export(A agg)} (note that
  * {@code Exportable<A,VR>} is defined with type parameters for the input and output
  * of this method.
  */
  @Override
  public Integer export(final Struct agg) {
    return agg.getInt64("sum") / agg.getInt64("count");
  }
  
}
```

**NOTE:** It should be possible to support arbitrary Java objects for aggregation without much
extra work. For the scope of this KLIP, however, supporting just KSQL types (including `Struct`) 
should suffice.

#### Alternative

If we expect UDAFs to commonly require exportable functionality, then we can make this change in a
backwards incompatible change and introduce the `export` (or `terminate`) method into the UDAF
interface directly.

## Test plan

Each of these improvements will come with some built-in UDF/UDAF examples. These examples will then
be comprehensively covered by both unit tests as well as corresponding Query Translation Tests.
Specifically, the UDFs described in the [Value](#value) section will all be implemented alongside
this KLIP. For the new UDAF complex aggregation, we will ensure that the generated topologies do not
include any unexpected steps such as a repartition.

## Documentation Updates

* The `Example UDF Class` in `udf.rst` will be extended to showcase the new features. Namely, we
will replace the multiply double method to include varargs:

>```java
>      @Udf(description = "multiply N non-nullable DOUBLEs.")
>      public double multiply(final double... values) {
>        return Arrays.stream(values).reduce((a, b) -> a * b);;
>      }
>```

* The `Supported Types` section in `udf.rst` will include `Struct` and have an updated note at the 
bottom which reads:

> Note: Complex types other than List, Map and Struct are not currently supported. Wildcard types
> for List and Map are supported, but component elements must be of one of the supported types.

* The `UDAF` section in `udf.rst` will have an additional section to describe usage of the 
`Exportable` interface:

> A UDAF can support complex aggregation types if it implements `io.confluent.common.Exportable`.
> This will allow your custom UDAF to convert some intermediate type used for aggregation to a 
> KSQL output value. For example:
>
>```java
>@UdfFactory(description = "Computes a running average")
>class AverageUdaf implements Udaf<Long, Struct>, Exportable<Struct, Long> {
>   
>  @Override
>  public Long export(Struct runningSum) {
>    return runningSum.getInt64("sum") / runningSum.getInt64("count");
>  }
>  
>   // ...
> }
>```
>
> You can then use this UDAF like any other: `SELECT AVERAGE(val) as avg FROM ...` and the output
> value will adhere to the export specification in place of the Udaf specification.

* For exportable UDAFs, we will also need to generate updated documentation for `DESCRIBE FUNCTION`
calls to make sure that the output value is not the intermediate value, but rather the final 
exported value. This is a straightforward change in compiling the generated code. For example:

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
>	Returns     : BIGINT
> // Note that the signature for this is actually Udaf<Long, Struct> as seen above
>```

* Syntax reference needs to be updated to reflect any new UDF/UDAFs that are implemented as part of
this KLIP.

# Compatibility implications

N/A

## Performance implications

* For Wildcard types, there may be performance implications if we need to infer the types during 
runtime. As discussed in the [Design](#design) section, this can be mitigated by caching the types
after first deserialization.
