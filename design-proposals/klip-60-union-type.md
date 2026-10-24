# KLIP 60 - Union Type

**Author**: Zach Young (@zachariahyoung)| 
**Release Target**: TBD| 
**Status**: In Discussion| 
**Status**: In Discussion| 
**Discussion**: NA

**tl;dr:** There is a desire from the community to be able to support multi-schema format within a single topic.  Two solutions for supporting this are available.  One is using the union type within Avro and Protobuf.  The other is called name strategy using schema registry.  This proposal will focus on the latter.
           

## Motivation and background

Currently, ksqlDB doesn't support a union type.  All three schema support union types.  This include Avro, Protobuf and JSON.

The main reason union type should be supported is because a common pattern used within eventing is something call event carrier state transfer or event notification.  This pattern can be broken down even further.  You can either send a course grained event which will include everthing
from an aggregate root. The other options is to send a fine grained event.  This is normally a smaller domain event off one of the enities under the main aggregate root.  The last pattern is called event notification.  This can also be used when only a smaller part of the payload is required and only a reference number.

Depending on the use case a developer may want to mix up these three patterns.  When it comes to stream processing some processing may need to consider all of these event the same even when only a few properties are different.


## What is in scope

Below is a list of items that should be considered for in scope.

* Query should be able to handle a single type within the union type list.
* Query should be able to handle multiple types within the union type list.  This should be configureable.  Either an include or exclude flag should be available.
* Aggergation queries should be able to handle all types within the union list.
* When processing multiple schemas, a single field may be missing in one schema and present in the other.  A custom UDF should be configureable to handle the missing field. 

## What is not in scope

Below is a list of items that should not be considered for in scope.

* Query results will not suppprt union type as outputs.  
* Depending on the scope of work.  RecordNameStrategy and TopicRecordNameStrategy will not be considered for mutli-schema processing.  


## Value/Return

The main value that this feature provides is being able to handle union types.

## Public APIS

_How does this change the public APIs? Does the KSQL query language change? Are we adding/dropping 
support for data formats? Are we adding / removing configurations? Etc._

## Design

Listed below are the example schemas we will be working with.  


Union Type

```
syntax = "proto3";
package company;

import "REFERENCE-INCIDENTCREATE-value";
import "REFERENCE-INCIDENTUPDATE-value";
import "REFERENCE-INCIDENTDELETE-value";

option java_package = "com.company.proto";
option java_multiple_files = true;

message CloudEventTypes {
  oneof oneof_type {
    .company.IncidentCreate create_event = 1;
	.company.IncidentUpdate update_event = 2;
    .company.IncidentDelete delete_incident_cloud_event = 3;
  }
}
```

Create Incident Event

```
syntax = "proto3";
package company;

import "google/protobuf/wrappers.proto";
import "google/type/date.proto";
import "google/type/timeofday.proto";

option java_package = "com.company.proto";
option java_multiple_files = true;

message IncidentCreate {
  .google.protobuf.StringValue occurrence_id = 1;
  .company.IncidentCloudEvent.AttributeType claim_type = 2;
  .google.type.Date incident_date = 3;
  .google.type.TimeOfDay incident_time = 4;
  .google.protobuf.StringValue location_description = 5;
  .google.protobuf.StringValue preventable_indicator = 6;
  .google.protobuf.StringValue report_by_name = 7;
  
  message AttributeType {
    .google.protobuf.StringValue code = 1;
    .google.protobuf.StringValue description = 2;
  }
}
```


Update Incident Event

```
syntax = "proto3";
package company;

import "google/protobuf/wrappers.proto";
import "google/type/date.proto";
import "google/type/timeofday.proto";

option java_package = "com.company.proto";
option java_multiple_files = true;

message IncidentCreate {
  .google.protobuf.StringValue occurrence_id = 1;
  .company.IncidentCloudEvent.AttributeType claim_type = 2;
  .google.type.Date incident_date = 3;
  .google.type.TimeOfDay incident_time = 4;
  .google.protobuf.StringValue location_description = 5;
  .google.protobuf.StringValue preventable_indicator = 6;
  
  message AttributeType {
    .google.protobuf.StringValue code = 1;
    .google.protobuf.StringValue description = 2;
  }
}
```

Delete Incident Event

```
syntax = "proto3";
package company;

import "google/protobuf/wrappers.proto";

option java_package = "com.company.proto";
option java_multiple_files = true;

message IncidentDelete {
  .google.protobuf.StringValue occurrence_id = 1;
}

```

A few notes on these events.  The first event is the union event.  It would contain all possible events.  The create and update event are event carrier state transfer type of events.  I would consider these to be course grained event and this is because incident is the aggerate root in this contrived example.

The only difference between the create and update event is one field is missing.  This is only missing to help with the design discussion.  Normally these events would be similar.  I would be more common to have finer grained event created off one of the entities under the aggregate root.  Since this simple aggregate doesn't have any entities it will not show case finer grained event.

The delete event would be example of what event notifcation type of event.




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
