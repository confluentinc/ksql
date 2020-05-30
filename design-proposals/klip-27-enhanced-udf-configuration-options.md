# KLIP 27 - Enhanced UDF Configuration Options

**Author**: Hans-Peter Grahsl (@hpgrahsl) | 
**Release Target**: ??? | 
**Status**: _Approved_ | 
**Discussion**: [GitHub PR](https://github.com/confluentinc/ksql/pull/5269)

**tl;dr:** This KLIP suggests to introduce a new configuration option in order to externally define UDF properties, that can be shared among a group / family of (custom) UDFs which logically belong together. At the same time, support for the `_global_` scope of configuration settings should get dropped.

## Motivation and background

Currently, ksqlDB supports two different ways to configure UDFs based on entries defined in `ksql-server.properties`:

* either the configuration properties are directly bound to a single, specific UDF class and applicable to potentially existing overloaded methods - e.g. `ksql.functions.<myudfname>.<mysetting>=foo`

* or we can have properties defined in a special scope, making them globally accessible - e.g. `ksql.functions._global_.<mysetting>=1234`

The latter approach means that properties are readable by any(!) other UDF that gets loaded during bootstrap. While this may or may not be an issue depending on the type of properties, in general, it can be considered less than ideal. For instance, this leads to security concerns for sensitive configuration properties such as secrets / tokens / passwords, which is why the `_global_` scope should get dropped.

## What is in scope

This KLIP aims to allow for multiple, yet specifically defined UDFs, to share configuration properties other than "misusing" the global properties scope.

## What is not in scope

The scope of this KLIP is not a complete rewrite of the current configuration mechanisms, but a modified implementation that suppport the sharing of configuration properties between specific (custom) UDFs.

## Value/Return

The main value of this KLIP lies in the fact that users can easily define certain configuration properties that are shared by N >= 2 UDFs. This can be done without specifying such properties multiple times, once for each UDF in need, or without "misusing the global scope", both of which can be considered workarounds at best.

## Public APIS

There should neither be public API changes nor any KSQL query language modifications necessary. Ideally, the chosen implementation allows for existing configurations to be upgraded / migrated to newer ksqlDB versions without changes for configurations tied to specific UDFs. 

**Note however, that for workloads which make use of the `_global_` scope it will lead to a breaking change since support for this  gets dropped.**

## Design

A simple approach is to introduce a wildcard character that can be used to do prefix matching of the UDFs' names that should be able to share certain configuration properties. For instance,

```properties
ksql.functions.my_udfgroup_*.some.value.x=foo
ksql.functions.otherudfs_*.other.value.y=1234
```

means, that based on the wildcard character `*` the setting `some.value.x` would be accessible by all UDFs which have a name starting with `my_udfgroup_`. The setting `other.value.y` is only available for the by all UDFs having a name starting with `otherudfs_*`.

## Test plan

Test and failure scenarios are rather straight-foward, focusing only on different ways to set and read configuration properties for UDFs. There shouldn't be a need for scale/load/performance testing because this KLIP doesn't affect any criticial areas in that regard.

## LOEs and Delivery Milestones

To be defined by product manager and team.

## Documentation Updates

The current documentation regarding configuration options for custom UDFs isn't overly specific but of course needs to be updated accordingly. **It's important to be very clear and call out the dropped support of the `_global_` configuration scope which leads to a breaking change for upgrading/migrating worklods.**

## Compatibility Implications

As the design proposal suggests to remove support of the `_global_` scope, this means that upgrading the ksqlDB version will lead to a breaking change in that regard. However, the implementation of the `*` wildcard prefix matching of UDFs' names should be carried out in a backwards compatible way such that it can reasonably support the "old" (i.e. current) and the "new" (i.e. what's proposed here) configuration options in parallel.

## Security Implications

There should be no negative implications regarding security. On the contrary, if the `_global_` scope cannot be "misused" any longer in order to (accidentally) share _sensitive_ configuration properties across all UDFs, it can even improve overall security a little bit.

## Rejected Alternatives

Configuration properties are initially set "without any scope". At this point, such properties aren't available to any UDFs at all. Only after defining an explicit scope by means of providing a list with all UDF names, will such properties become available to the corresponding functions. For instance, 

```properties
ksql.functions.some.value.x=foo
ksql.functions._scope_.some.value.x=my_udfgroup_a,otherudfs_b
```

means that based on the explicitly defined `_scope_` for `some.value.x`, this property would be accessible by the two UDFs specified, namely, `my_udfgroup_a` and `otherudfs_b`. A setting isn't available to any UDFs in case its scope has not been defined or is empty.

It would of course be an option to also allow for simple wildcard matching, or complex regex rules for any UDF name entry defined in the `_scope_` list.
