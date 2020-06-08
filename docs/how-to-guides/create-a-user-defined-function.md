# How to create a user-defined function

## Context

You have a piece of logic for transforming or aggregating your events that ksqlDB can’t currently express. You want to extend ksqlDB to apply that logic to your streams and tables of events. ksqlDB exposes interfaces so that you can add new logic through Java programs. This functionality is called user-defined functions.

## In action

```java
package my.company.ksqldb.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two non-nullable INTs.")
  public long multiply(@UdfParameter(value = "v1") final int v1, @UdfParameter(value="v2") final int v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two non-nullable BIGINTs.")
  public long multiply(@UdfParameter(value="v1") final long v1, @UdfParameter(value="v2") final long v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two non-nullable DOUBLEs.")
  public double multiply(@UdfParameter(value="v1") final double v1, @UdfParameter(value="v2") double v2) {
    return v1 * v2;
  }
}
```
## Set up a Java project

## Implement the classes

### Functions

### Tabular functions

### Aggregation functions

## Make the jar available

## Invoke the functions