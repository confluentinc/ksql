# KLIP Number - Title

**Author**: _your name and github user name here_ | 
**Release Target**: _release version this feature is targeted at_ | 
**Status**: _In Discussion_ or _Design Approved_ or _In development_ or _Merged_ | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** _The summary of WHY we we should do this, and (if possible) WHO benefits from this.  If this unclear 
           or too verbose, it is a strong indication that we need to take a step back and re-think the 
           proposal._
           
_Example: "Rebalancing enables elasticity and fault-tolerance of Streams applications. However, as 
of today rebalancing is a costly operation that we need to optimize to achieve faster and more 
efficient application starts/restarts/shutdowns, failovers, elasticity."._

## Motivation and background

_What problem are you trying to solve and why. Try to describe as much of the context surrounding 
the problem as possible._

## What is in scope

_What requirements are you addressing. This should cover both functional and non-functional requirements. The non-functional requirements include things like expected performance profile of the feature, impact on efficiency, impact on scalability, etc._

## What is not in scope

_What we explicitly do not want to cover in this proposal, and why._

> We will not ______ because ______.  Example: "We will not tackle Protobuf support in this proposal 
> because we must use schema registry, and the registry does not support Protobuf yet."

## Value/Return

_What's the value for the end user. Imagine that everything in this proposal would be readily 
available.  Why would our users care?  And specifically which users would care?  Can they now 
implement new use cases that weren't possible before?   Can they implement existing use cases much 
faster or more easily?_

## Public APIS

_How does this change the public APIs? Does the KSQL query language change? Are we adding/dropping 
support for data formats? Are we adding / removing configurations? Etc._

## Design

_How does your solution work? This should cover the main data and control flows that are changing._

## Test plan

_What are the failure scenarios you are going to cover in your testing? What scale testing do you plan to run? What about peformance and load testing? It goes 
without saying that most classes should have unit tests._

## LOEs and Delivery Milestones

_Large features should be built such that incremental value can be delivered along the way. For the feature you are building, outline the delivery milestones which result in concrete user facing value. If some milestones have dependencies on other ongoing work or planned changes, call that out here and include any relevant links for those dependencies._

_Small features--say those which can complete in two or three weeks--may have only one delivery milestone._

_Additionally, for each milestone, try to break the work down into more granular tasks. Make sure to include tasks for testing, documentation, etc., in addition to the core development tasks. Ideally, tasks should be scoped such that each task is at most 1 person-week, where a person-week is calendar time and accounts for time not spent actually developing the proposed feature._

_Breaking a feature into milestones and tasks need not be done when first proposing a KLIP, since the scope, functionality, and design may change through the discussion. However, these breakdowns would ideally be provided as the KLIP is getting finalized and before execution begins._

## Documentation Updates

_What changes need to be made to the documentation? For example_

* Do we need to change the KSQL quickstart(s) and/or the KSQL demo to showcase the new functionality? What are specific changes that should be made?
* Do we need to update the syntax reference?
* Do we need to add/update/remove examples?
* Do we need to update the FAQs?
* Do we need to add documentation so that users know how to configure KSQL to use the new feature? 
* Etc.

_This section should try to be as close as possible to the eventual documentation updates that 
need to me made, since that will force us into thinking how the feature is actually going to be 
used, and how users can be on-boarded onto the new feature. The upside is that the documentation 
will be ready to go before any work even begins, so only the fun part is left._

## Compatibility Implications

For any backward-incompatible changes introduced by this KLIP, it is required that the specific upgrade steps that users will need to perform are included in this section. For example, if there are any syntax changes, how specifically should users adapt their existing SQL statements to work with the new version? If the change is something other than a syntax change, what steps must users take to make their existing workloads run on the new version? This information will ultimately be used to inform users of upcoming backward-incompatible changes with as much advanced notice as possible.

Please include a reasonable amount of commentary around the upgrade steps in order to help users understand their purpose and necessity.

_Will the proposed changes break existing queries or work flows?_

_Are we deprecating existing APIs with these changes? If so, when do we plan to remove the underlying code?_

_If we are removing old functionality, what is the migration plan?_

## Security Implications

_Are any external communications made? What new threats may be introduced?_ ___Are there authentication,
authorization and audit mechanisms established?_
