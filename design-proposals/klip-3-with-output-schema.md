# 3 - WITH OUTPUT_SCHEMA

Author: [Tobias Hauck][TobiasHauck]

Release target: 5.3

Status: In Discussion

<!-- TODO: replace with link to PR -->
**Discussion**: [link][design_pr]

## tl;dr

Many tickets address issues with data types, especially when ksql data is sinked back to a topic. By giving more control over schema generation a lot of issues will be solved so that a broader audience is able to use KSQL for their specific use cases.

## Motivation and background

Currently connect schemas pushed to the schema registry are inferred by KSQL which means all field names become uppercase (except you explicitly write SELECT id as `id`, ...), all fields become optional and Maps become arrays of MapEntries which is a result of all fields becoming optional. This behavior is intended and not a bug.

But sometimes it's undesirable. Let's say you want to filter a few properties of an existing topic containing avro data and write the result into a new topic, you expect that the new topic has the same field definitions than the filtered one. But that's not the case. Even worse: In my specific case I was not able to use KSQL and had to go for something else, because the new map schema was not compatible to the one I expected which caused issues in the application using the target topic. So we should give more control over the resulting data schema.

Example:

```
CREATE TABLE FOO
WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO', OUTPUT_SCHEMA='bar')
AS SELECT * FROM BAR;
```

## What is in scope

I suggest to introduce a new ``WITH`` property called ```OUTPUT_SCHEMA``` that can be used to explain the query which exact schema to use for the resulting topic and its data. So this new property is only available in ```CREATE TABLE/STREAM AS SELECT``` clauses.

## What is not in scope

We will not add support for any other ```VALUE_FORMAT``` than ```AVRO```, because the schema evolution issue only affects AVRO.

## Value/Return

The user can decide which resulting schema to go for. This means that many will no longer have a headache when it comes to deriving schemas. Sometimes users don't want optional fields because it's breaking their type validation if the schema registry is put in a larger context than just ksql - for example a REST API. Mapping maps to arrays of MapEntries is not a valid type cast even if there are good reasons to do so, for example for joined streams. So some use cases are currently not implementable, for example cloning a topic using KSQL. I personally cannot use the current version of KSQL for my use case.

So the overall value is that you can use KSQL better in combination with Kafka Rest Proxy.

## Public APIS

The API is extended by a new ```WITH``` property called ```OUTPUT_SCHEMA``` that is only available for ```CREATE STREAM/TABLE AS SELECT``` and only in combination with the ```VALUE_FORMAT``` ```AVRO```.

## Design

The new ```WITH``` property is analyzed by the Analyzer and stored in the Query Analysis. That information is processed by the query engine and used by the AvroSerializer (KsqlAvroTopicSerDe). KsqlAvroTopicSerDe uses the schema registry client to get the latest metadata of the related output schema and uses this schema instead of the current one.

## Test plan

I'm planning to write unit tests and extend the current integration tests with the new functionality. That should be enough.

## Documentation Updates

* Do we need to change the KSQL quickstart(s) and/or the KSQL demo to showcase the new functionality? What are specific changes that should be made?

- Yes we should show it immediately in the quickstart(s).

* Do we need to update the syntax reference?

- Yes.

* Do we need to add/update/remove examples?

- Yes.

* Do we need to update the FAQs?

- Could be an option. I go for "yes".

* Do we need to add documentation so that users know how to configure KSQL to use the new feature?

- Also a yes. We need the users to be informed so that they stop asking the same questions again and again.

# Compatibility implications

* Will the proposed changes break existing queries or work flows?

- No, it's only an extension.

Are we deprecating existing APIs with these changes?

- No.

## Performance implications

The performance should not be affected much as the schema is only queried for exactly one time and then it's in the application's cache.
