# KLIP Number - Title

Author: _your name and github user name here_

Release target: _release version this feature is targeted at_

Status: _In Discussion | Design Approved | In development | Merged_

Discussion: _link to the design discussion PR_

## tl;dr

_The summary of WHY we we should do this, and (if possible) WHO benefits from this.  If this unclear or too verbose, it is a strong indication that we need to take a step back and re-think the proposal._

_Example: "Rebalancing enables elasticity and fault-tolerance of Streams applications. However, as of today rebalancing is a costly operation that we need to optimize to achieve faster and more efficient application starts/restarts/shutdowns, failovers, elasticity."._

## Motivation and background

_What problem are you trying to solve and why. Try to describe as much of the context surrounding the problem as possible._

## What is in scope

_What we do want to cover in this proposal._

## What is not in scope

_What we explicitly do not want to cover in this proposal, and why._

> We will not ______ because ______.  Example: "We will not tackle Protobuf support in this proposal because we must use schema registry, and the registry does not support Protobuf yet."

## Value/Return

_What's the value for the end user. Imagine that everything in this proposal would be readily available.  Why would our users care?  And specifically which users would care?  Can they now implement new use cases that weren't possible before?   Can they implement existing use cases much faster or more easily?_

## Public APIS

_How does this change the public APIs? Does the KSQL query language change? Are we adding/dropping support for data formats? Are we adding / removing configurations? Etc._

## Design

_How does your solution work?_

## Test plan

_What tests do you plan to write?  What are the failure scenarios that we do / do not cover? It goes without saying that most classes should have unit tests. This section is more focussed on the integration and system tests that you need to write to test the changes you are making._

## Documentation Updates

_What changes need to be made to the documentation? For example_

* Do we need to change the KSQL quickstart(s) and/or the KSQL demo to showcase the new functionality? What are specific changes that should be made?
* Do we need to update the syntax reference?
* Do we need to add/update/remove examples?
* Do we need to update the FAQs?
* Do we need to add documentation so that users know how to configure KSQL to use the new feature? 
* Etc.

_This section should try to be as close as possible to the eventual documentation updates that need to me made, since that will force us into thinking how the feature is actually going to be used, and how users can be on-boarded onto the new feature. The upside is that the documentation will be ready to go before any work even begins, so only the fun part is left._

# Compatibility implications

_Will the proposed changes break existing queries or work flows?_

_Are we deprecating existing APIs with these changes? If so, when do we plan to remove the underlying code?_

_If we are removing old functionality, what is the migration plan?_

## Performance implications

_Will the proposed changes affect performance, (either positively or negatively)._
