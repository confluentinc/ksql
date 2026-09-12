# KLIP 66 - Multiple parameter UDAFs

**Author**: @jzaralim | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** _Enable users to create UDAFs with multiple parameters._

## Motivation and background

Currently, UDAFs can only accept a single column to aggregate. Users have shown interest
([#5747](https://github.com/confluentinc/ksql/issues/5747), [#7435](https://github.com/confluentinc/ksql/issues/7435), [#5300](https://github.com/confluentinc/ksql/issues/5300))
in aggregating over multiple columns. The workaround is to combine all of the required columns into a `Struct` and then implementing a `UDAF` to
work with `Struct` with that specific schema. This is tedious, and requires the user to implement a new version of their function for every schema they need to work with.

## What is in scope

* UDAFs that accept variadic parameters: `example(col_int_1, col_int_2, col_int_3...) -> result`
* UDAFs that accept a specific number of parameters, potentially of different types: `example(col_int, col_string) -> result`
* Variadic TopK - a version of `TOPK` that returns the top K values of one column, along with the values of other columns in a `Struct` from those records.
* `correlation(double, double)` - a UDAF that computes the correlation coefficient of two columns.

## What is not in scope

* UDAFs that return multiple columns. 

## Design

### Variadic parameters

We will add a new class called `MultipleArgs` for users to represent variadic arguments, which is a wrapper for `List`:

```
public class MultipleArgs<T> {

  private final List<T> values;

  public MultipleArgs(final List<T> values) {
    this.values = values;
  }

  public List<T> getValues() {
    return values;
  }
}
```

The `List` contains the values of the columns specified in the function call. An example usage is:
```
// Returns the sum of all the values from multiple colunms
private static <Integer> Udaf<MultipleArgs<Integer>, Long, Long> MultiColumnSum() {
  return new Udaf<MultipleArgs<Integer>, Long, Long>() {

    @Override
    public List<Integer> initialize() {
      return 0;
    }

    @Override
    public List<Integer> aggregate(MultipleArgs<Integer> current, Long aggregate) {
      return aggregate + current.getValues().stream().reduce(0, Integer::sum);
    }

    @Override
    public List<Integer> merge(Long aggOne, Long aggTwo) {
      return aggOne + aggTwo;
    }

    @Override
    public Long map(Long agg) {
      return agg;
    }
  };
}
```

To create a function that accepts variadic parameters of any type, the `MultipleArgs` type parameter can be set to `Object`.

Alternatively, we could use the [Apache Commons tuple library](https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/tuple/package-summary.html),
but it only has `Pair` and `Triple`.

### Specific parameters

In order to specify multiple parameters of different types, we will use the [javatuples](https://www.javatuples.org/) library.
For example, a UDAF that requires one integer parameter and one string parameter can be implemented as follows:

```
private static <Integer> Udaf<Pair<Integer, String>, List<Integer>, Long> example() {
  return new Udaf<Pair<Integer, String>, List<Integer>, Long>() {

    @Override
    public List<Integer> initialize() {
      return null;
    }

    @Override
    public List<Integer> aggregate(Pair<Integer, String> current, List<Integer> aggregate) {
      return null;
    }

    @Override
    public List<Integer> merge(List<Integer> aggOne, List<Integer> aggTwo) {
      return null;
    }

    @Override
    public Long map(List<Integer> agg) {
      return null;
    }
  };
}
```

[Javatuples](https://www.javatuples.org/) only go up to 10 parameters, but if a user wants to use more then they could use `VariadicArgs<Object>` and manually add type checking to the `aggregate` function.

### Rejected alternatives

One alternative method for users to represent multiple parameters of various types is by adding another interface called `MultiVarUdaf` that extends `Udaf`.
A user would implement this interface with an `aggregate` function of the form `aggregate(I col1, J col2, K col3 ... A agg)`. The original `aggregate(I current, A aggregate)`
would be implemented to reflect on the class, find the user implemented `aggregate(I col1, J col2, K col3 ... A agg)` function, and then type check
the columns.

Here's an example of a working implementation of `aggregate`.

```
@Override
  public  A aggregate(VariadicArgs current, A aggregate) {
    final Class[] classes = new Class[current.getValues().size() + 1];
    final Object[] args = new Object[current.getValues().size() + 1];
    for (int i = 0; i < current.getValues().size(); i++) {
      classes[i] = current.getValues().get(i).getClass();
      args[i] = current.getValues().get(i);
    }
    classes[current.getValues().size()] = aggregate.getClass();
    args[current.getValues().size()] = aggregate;
    try {
      final Method aggMethod = this.getClass().getMethod("aggregate", classes);
      return (A) aggMethod.invoke(this, args);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return null;
    }
  }
```

## Test plan

Besides the usual unit + integration tests, we will be adding new UDAFs to demonstrate that multiple parameter UDAFs work.

## LOEs and Delivery Milestones

1. Implement support for variadic UDAFs
2. Implement support for tuples
3. New UDAFs

## Documentation Updates

* Docs for new UDAFs
* The [developer guide](https://docs.ksqldb.io/en/latest/how-to-guides/create-a-user-defined-function/) for creating functions will need to be updated to include instructions for creating multiple parameter UDAFs
* We should add the new parameter types to the [reference page](https://docs.ksqldb.io/en/latest/reference/user-defined-functions) as well as a section explaining what these types are.

## Compatibility Implications

None. This will not change how single parameter UDAFs work.

## Security Implications

None.
