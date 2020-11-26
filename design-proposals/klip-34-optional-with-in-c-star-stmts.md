# KLIP 34 - Optional `WITH` clause in `CREATE STREAM` and `CREATE TABLE` statements

**Author**: @big-andy-coates | 
**Release Target**: TBD | 
**Status**: In Discussion | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** `CREATE STREEAM` and `CREATE TABLE` statements currently require a `WITH` clause as the
           user must supply the topic name. We propose allowing users to set a topic naming strategy
           via config, making the `WITH` clause optional. Thereby making the `CREATE` statement more
           Ansi SQL and hence more intuitive to new users. 
           
## Motivation and background

Both `CREATE STREAM` and `CREATE TABLE` statements must currently have a `WITH` clause as there is
a single required properties:

* `KAFKA_TOPIC`: sets the name of the Kafka topic the data is, or will be, stored in.

For example:

```sql
CREATE TABLE FOO (
   ID BIGINT PRIMARY KEY,
   VAL STRING
 ) WITH (
   KAFKA_TOPIC='foo'
);
```

Switching this properties to _optional_ would allow more natural SQL syntax:

```sql
CREATE TABLE FOO (
   ID BIGINT PRIMARY KEY,
   VAL STRING
);
```

This is more intuitive and familiar to new users with a SQL background.

## Why required `KAFKA_TOPIC`?

It's very common to see `CREATE` statements where the `KAFKA_TOPIC` name matches the name of the stream
or table being created, maybe with different case.

`KAFKA_TOPIC` is already optional for `CREATE STREAM AS SELECT` and `CREATE TABLE AS SELECT` statements.

There seems no reason to _require_ users to supply the topic name where it matches the name of the 
created stream or table.

## What is in scope

* A new config to set a strategy for matching a source name to a Kafka topic. 
* A new public interface, `SourceTopicNamingStrategy`, which users can use to implement their own 
  name resolution strategy.
* New inbuilt resolution strategies
* Making `KAFKA_TOPIC` property in the `WITH` clause of CT/CS statements optional.

## What is not in scope

Anything and everything else =)

## Value/Return

Clearer and cleaner syntax, with less unnecessary typing. Lowers barrier of entry for new users who
are already familiar with Ansi SQL.

Removes clutter from examples, demos and design docs.

## Public APIS

### Server configs

A new `ksql.persistence.source.topic.naming.strategy` configuration option will be added. This 
configuration can be set globally, within the application property file, or locally, via the 
`SET` command.

The configuration will determine what class to instantiate to resolve a new source's topic name.
The class must implement `SourceTopicNamingStrategy`. 

The configuration will default to `CaseInsensitiveSourceTopicNamingStrategy`. 

### CREATE properties

The `KAFKA_TOPIC` property will no longer be required in the `WITH` clause of `CREATE TABLE` and 
`CREATE STREAM` statements.

Users can still choose to supply an explicit topic name.

Setting the resolution strategy to `ExplicitSourceTopicNamingStrategy` will make `KAFKA_TOPIC` a
required explicit property again.

### `SourceTopicNamingStrategy` interface and implementations

A simple interface to determine the topic name from the source name and the set of known topic names:

```java
public interface SourceTopicNamingStrategy {

  String resolveExistingTopic(SourceName sourceName, Set<String> topicNames);
}
```

Three implementations will be provided out of the box:

* `ExplicitSourceTopicNamingStrategy`: Will thrown exception on any invocation, effectively making `KAFKA_TOPIC` a required property.
* `CaseSensitiveSourceTopicNamingStrategy`: Will match a topic name that _exactly_ matches the source name. 
* `CaseInsensitiveSourceTopicNamingStrategy`: Will perform case-insensitive matching, preferring an exact match, and throwing on multiple if no exact match exists.

## Design

The rest server will utilise the existing `TopicCreateInjector` to invoke the configured 
`SourceTopicNamingStrategy` to determine the name of the topic, and to bake this topic name into
the plan being written to the command topic.  This will allow other parts of the code to expect
the topic name to always be set, minimising the surface area of the change.

## Test plan

Some of the existing RQTT and QTT tests will be updated to use the new syntax. New positive and 
negative QTT tests will be added to cover the different configuration options. 

## LOEs and Delivery Milestones

Single deliverable. LOE low... a day?

## Documentation Updates

Update of `CREATE TABLE` and `CREATE STREAM` docs to reflect that `KAFKA_TOPIC`
is potentially optional, depending on `ksql.persistence.source.topic.naming.strategy` setting.

## Compatibility Implications

None.

## Security Implications

Care needs to be taken to ensure the topic name is resolved before any access checks are performed. 
