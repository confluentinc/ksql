# KLIP-30 - Lambda Expressions

**Authors**: derekjn, blueedgenick |
**Release Target**: N/A |
**Status**: _In Discussion_ |
**Discussion**: N/A

**tl;dr**: The introduction of lambda functions would bridge the gap somewhat between builtins and UDFs.

## Motivation and background

While ksqlDB’s UDF interface makes it relatively easy for users to add their own functionality to invoke within their queries, the process of implementing and deploying a UDF is not particularly straightforward. Furthermore, the deployment of UDFs may not even be an option in some environments. Without UDFs, users are at the mercy of ksqlDB’s builtins, which may not always provide enough functionality for them to solve their specific problems. This can be particularly problematic with collections (i.e. `Array` and `Map`), as it can be awkward to work with all of their individual values within a single query. However, we can mitigate this limitation somewhat by empowering users to express user-defined functionality in a way that doesn’t require them to implement full-fledged UDFs.

Introducing lambda functions would enable users to express simple inline functions that can be applied to input values in various ways. For example, lambda functions could be applied to each element of a collection, resulting in a transformed output collection. Lambda functions could also be used to filter the elements of a collection, or even reduce a collection down to a single output value.

The remainder of this document will propose the addition of lambda functions into ksqlDB’s grammar and execution engine.

## Scope

### What is in scope 
* Syntax for describing lambda functions will be proposed
* Lambda invocation functions will be proposed

## What is not in scope
* Implementation details will not be proposed 
* Level of effort will not be estimated


## Value/return

The introduction of lambda functions will ultimately empower ksqDB users to solve more problems with less effort. Many of the UDFs we’ve seen users write are designed to apply relatively simple functionality to the elements of `Arrays` and `Maps`. Lambda functions are well-suited to solve this class of problems in a very easy and intuitive way.

Lambda functions will also enable some users to use ksqlDB in environments that do not allow for the deployment of UDFs.
Design

## Public APIs

There are two aspects of the user-facing lambda function interface: syntax for describing the actual lambda functions, and a way to specify how to apply these functions to input values.

## Design

### Syntax

It would probably be ideal to use Java-style lambda syntax, but we already use the `->` symbol to represent a `Struct` field lookup. Giving `->` a double meaning (if that’s even possible) would create ambiguity and complexity in the grammar/parser. We therefore propose using the `=>` symbol to signify a lambda function:

```
arg => expr
```

This expression represents the following:

* `arg` is the argument passed into the scope of each invocation of the lambda function.
* `expr` is the "body" of the lambda function. Each invocation of the lambda function will evaluate this expression and return the result.

Multiple arguments should also be allowed for lambda functions. When multiple arguments are used, they should be wrapped in parentheses:

```
(x, y) => expr
```

**Allowed expressions**:

The body of each lambda function should simply be an expression that could be used in a `SELECT` expression list, with some exceptions depending on the invocation function:

* Aggregates should not be allowed (please see *Open questions* section)
* `SELECT` subqueries should not be allowed

Given these restrictions, the following examples would be valid lambda functions:

**Examples**:

* `x => x + 1`
* `s => UCASE(s)`
* `(x, y) => x + y`
* `(x, y) => CASE WHEN x IS NULL THEN y ELSE x + y END`

### Invocation functions

Lambda functions should require a specific **invocation function**, which tells ksqlDB how to apply the given lambda function to an input value, and what kind of result to return. The following invocation functions are proposed for the initial support for lambda functions:

- `transform_array(arr, x => y)` - Applies the given lambda function to each element of the input `Array`, returning a new `Array` containing the transformed output.

- `transform_map(map, (k, v) => x, y)` - Applies the given lambda function to each key-value pair of the input `Map`, returning a new `Map` containing the transformed key-value pairs.

- `filter_array(arr, x => bool)` - Filters the input `Array` using the given lambda function. A new `Array` is returned, containing only values for which the lambda function evaluated to `true`.

- `filter_map(map, (k, v) => bool)` - Filters the input `Map` using the given lambda function. A new `Map` is returned, containing only the key-value pairs for which the lambda function evaluated to `true`.

## Documentation updates

We must document and provide examples for the two core aspects of lambda functions:

- Lambda function syntax
- Invocation functions

## Compatibility implications

Lambda functions would be additive to ksqlDB's grammar and should therefore not introduce any incompatibilities into the grammar or public APIs. However, it is not currently known if a lambda function implementation would require backward-incompatible changes to ksqlDB's internals.

## Security implications

Since lambda functions would only allow SQL expressions that can already be used within queries, no new security implications should be introduced.

## Open questions

**1. Would it be possible to allow aggregates within a particular invocation function? For example,**

- `agg_array(arr, x => sum(x))`

If this is feasible then it would likely be worth doing. Without support for aggregates via `reduce`, users will not be able to easily perform any aggregation over a collection.

**2. Would allowing lambda functions to be applied to types other than `Array` or `Map` provide any value?**

**3. Will the lambda function implementation require backward-incompatble changes to ksqlDB's internals?**