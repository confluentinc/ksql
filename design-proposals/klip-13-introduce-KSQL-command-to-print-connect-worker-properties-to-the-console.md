# KLIP 13 - Introduce KSQL command to print connect worker properties to the console

**Author**: alex-dukhno | 
**Release Target**: N/A | 
**Status**: _In Discussion_ | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** The best tools are ones that greatly support users workflow. As of today users couldn't 
get connect worker properties in `ksql cli`, thus forcing them to switch to editor to open up that 
locates under the path `ksql.connect.worker.config`. Having a command to get that information could 
lead to a better user experience using `ksql cli`.

## Motivation and background

As is mentioned in [issue](https://github.com/confluentinc/ksql/issues/3777) currently user doesn't
have visibility into connect worker configuration. When something goes wrong one of the first action 
is to check if configuration is done right. Currently, users, if they need to see connect worker configuration,
have to use editors to open up the file at `ksql.connect.worker.config` location, which leads to 
context switch and loosing attention. That could be unpleasant experience during troubleshooting, 
especially on a production system.

## What is in scope

We would like to extend `(LIST|SHOW) PROPERTIES` `KSQL` command to `(LIST|SHOW) PROPERTIES (CONNECT)?` 
that could be run from `ksql cli` and prints connect worker configuration.

## What is not in scope

It is not a goal of the KLIP to implement general command (or extension) that could provide configuration
 information of other `ksql` component. General solution or other extensions require their own KLIP(s).

## Value/Return

The functionality will provide ability for a user to quickly print and check connect worker configuration
from the `ksql cli` that gives better user experience.

## Public APIS

We need to make small change to KSQL query language by introducing new keyword `CONNECT`.

## Design

One of the way to implement it is to add a `flag` field to `ListProperties` statement class. The `flag` 
will be set if key word `(LIST|SHOW) PROPERTIES` follows by `CONNECT` keyword. Base on `flag` print 
connect worker or all configuration properties.

## Test plan

New unit/module level tests will be written as appropriate.

## Documentation Updates

Documentation should reflect that new extension is introduced to `(LIST|SHOW) PROPERTIES` `KSQL` command.

# Compatibility Implications

The only implication is that we are introducing new keyword `CONNECT` that should be supported.

## Performance Implications

The new features will not affect performance of existing KSQL apps

## Security Implications

If connect worker can have security related properties they have to be obfuscated.
