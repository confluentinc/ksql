# KLIP 61 - Bootstrapped TABLE subscriptions

**Author**: Natea Eshetu Beshada @nateab |
**Release Target**: 0.27.0 |
**Status**: _In Discussion_ |
**Discussion**: _link to the design discussion PR_

**tl;dr:** We want users to be able to very easily with a single query get all past data from a table and 
simultaneously subscribe to any future changes to that table by specifying 'EMIT ALL'.

## Motivation and background

ksqlDB is a streaming database that allows users to work with data in motion (stream processing) and data at rest (databases).
Thus, we have push queries which let you subscribe to a table as it changes in real-time, and also pull 
queries which retrieves the current state of the table, similar to a traditional database query. To further cement ksqlDB's
positioning at the intersection of stream processing and traditional databases, it is necessary to develop features that 
seamlessly combine the two. Adding bootstrapped table subscriptions to ksqlDB achieves this purpose by allowing users to query their 
data at rest up until now, while also subscribing to any later changes from data that is in motion. 

Users can try to achieve this functionality by simply combining a table pull query followed by a table push query, but this 
could result in gaps in the returned data since there are no guarantees that the push query will not miss any data after the 
pull query is executed. Implementing this feature, we will keep track off the offsets under the hood for the user so that 
we can guarantee there is no missing data. It will also be more clean and intuitive for users to have to only use one query
to achieve this.

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

_Will the proposed changes break existing queries or work flows?_

_Are we deprecating existing APIs with these changes? If so, when do we plan to remove the underlying code?_

_If we are removing old functionality, what is the migration plan?_


## Security Implications

_Are any external communications made? What new threats may be introduced?_ ___Are there authentication,
authorization and audit mechanisms established?_
