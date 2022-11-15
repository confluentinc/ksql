# KLIP 27 - Enhanced UDF Configuration Options

**Author**: Hans-Peter Grahsl (@hpgrahsl) | 
**Release Target**: ??? | 
**Status**: _Approved_ | 
**Discussion**: [GitHub PR](https://github.com/confluentinc/ksql/pull/5269)

**tl;dr:** This KLIP suggests to introduce a new configuration option in order to define function properties, that can be shared among a group / family of (custom) UDFs, UDAFs and/or UDTFs which logically belong together.

## Motivation and background

Currently, ksqlDB supports two different ways to configure (custom) functions based on entries defined in `ksql-server.properties`:

* either the configuration properties are directly bound to a single, specific UDF class and applicable to potentially existing overloaded methods - e.g. `ksql.functions.<myudfname>.<mysetting>=foo`

* or we can have properties defined in a special scope, making them globally accessible - e.g. `ksql.functions._global_.<mysetting>=1234`

The latter approach means that properties are readable by any(!) other function that gets loaded during bootstrap. While this may or may not be an issue depending on the type of properties, in general, it can be considered less than ideal. For instance, this leads to security concerns for sensitive configuration properties such as secrets / tokens / passwords.

## What is in scope

This KLIP aims to allow for multiple, yet specifically defined functions - including UDFs, UDAFs and UDTFs - to share configuration properties other than "misusing" the global properties scope.

## What is not in scope

The scope of this KLIP is not a complete rewrite of the current configuration mechanisms, but a modified implementation that suppports the sharing of configuration properties between specific (custom) functions (UDFs, UDAFs, UDTFs).

## Value/Return

The main value of this KLIP lies in the fact that users can easily define certain configuration properties that are shared by N >= 2 functions. This can be done without specifying such properties multiple times, once for each function in need, or without "misusing the global scope", both of which can be considered workarounds at best.

## Public APIS

There should neither be public API changes nor any KSQL query language modifications necessary. Ideally, the chosen implementation allows for existing configurations to be upgraded / migrated to newer ksqlDB versions without changes for configurations tied to specific functions.

## Design

A simple approach is to extend the function meta-data and add a _groups_ option to the `@UdfDescription`, `@UdafDescription` and `@UdtfDescription` annotations, which takes list of strings. Each string entry in _groups_ explicitly defines a name used to logically group functions for which to share configuration properties.

Given the following example:

```java
@UdfDescription(
    name = "someudf",
    description = "...",
    author = "...",
    version = "...",
    groups = {"shareit"/*,"..."*/})
public class SomeUdf implements Configurable {
    //...
}
```

allows to configure the UDF named _someudf_ using `ksql.functions.someudf.my.value.x=foo` (basic, single-UDF config) or `ksql.groups.functions.shareit.my.value.x=foo` (grouped config across several functions declaring the same group name(s)). This distinction also makes it obvious, whether configuration properties are set for single functions or several grouped functions respectively.

A drawback of the meta-data driven approach is that the group information must be declared in the code. If this restriction turns out to be an issue, a future enhancement could allow to explicitly define the group information in the configuration properties like so: `ksql.groups.define.group_name=...,...` which for the particular example above would read `ksql.groups.define.shareit=someudf`.

The benefits of this design are:

* full backwards compatibility because _groups_ config meta-data is a new option
* the default and most common case is to configure a single function which means that it stays simple for users
* it is easy to add new configurations
* the grouping is neither derived from the name itself (e.g. prefix matching) nor is it tied to java package naming convention

## Test plan

Test and failure scenarios are rather straight-foward, focusing only on different ways to set and read configuration properties for functions (UDFs, UDAFs, UDTFs). There shouldn't be a need for scale/load/performance testing because this KLIP doesn't affect any criticial areas in that regard.

## LOEs and Delivery Milestones

To be defined by product manager and team.

## Documentation Updates

The current documentation regarding configuration options for custom functions isn't overly specific but of course needs to be updated accordingly.

## Compatibility Implications

The design proposal suggests a solution that should not lead to breaking changes for users. Thus, the implementation should be carried out in a backwards compatible way such that it can reasonably support the "old" (i.e. current) and the "new" (i.e. what's proposed here) configuration options in parallel.

## Security Implications

There should be no negative implications regarding security. On the contrary, if the `_global_` scope doesn't have to be "misused" any longer in order to (accidentally) share _sensitive_ configuration properties across all functions, it can even improve overall security a little bit.

## Rejected Alternatives

### Alternative 1:

One way to do this is to introduce a wildcard character that can be used to do prefix matching of the UDFs' names that should be able to share certain configuration properties. For instance,

```properties
ksql.functions.my_udfgroup_*.some.value.x=foo
ksql.functions.otherudfs_*.other.value.y=1234
```

means, that based on the wildcard character `*` the setting `some.value.x` would be accessible by all UDFs which have a name starting with `my_udfgroup_`. The setting `other.value.y` is only available for the by all UDFs having a name starting with `otherudfs_*`.

### Alternative 2:

Configuration properties are initially set "without any scope". At this point, such properties aren't available to any UDFs at all. Only after defining an explicit scope by means of providing a list with all UDF names, will such properties become available to the corresponding functions. For instance, 

```properties
ksql.functions.some.value.x=foo
ksql.functions._scope_.some.value.x=my_udfgroup_a,otherudfs_b
```

means that based on the explicitly defined `_scope_` for `some.value.x`, this property would be accessible by the two UDFs specified, namely, `my_udfgroup_a` and `otherudfs_b`. A setting isn't available to any UDFs in case its scope has not been defined or is empty.

It would of course be an option to also allow for simple wildcard matching, or complex regex rules for any UDF name entry defined in the `_scope_` list.
