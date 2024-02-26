# Query Execution Plans

Author: Rohan Desai (rodesai) |
Release target: 5.4 |
Status: _Merged_ |
Discussion:

## Motivation and background

One of the main differences between KSQL and a database is that KSQL queries are long-running. A database query is issued by a user, executes, and completes. A KSQL query usually has no defined end time. It keeps running until a user explicitly asks KSQL to stop running it. This is challenging because KSQL needs to ensure that it correctly runs queries across software versions, even as the engine that builds execution plans changes.

As an example of what can go wrong, consider the query:

`CREATE TABLE AGG AS SELECT X, Y, COUNT(*) FROM SOURCE;`

In today's KSQL, the key for AGG is computed by concatenating the string forms of X and Y. In the future we may wish to create a structured key instead. However, we need to be sure to continue using the concatenated string format for queries that were started before that change was made.

Our current approach to this problem is to do our best not to break compatibility by toggling incompatible behavior on compatibility-breaking configs, and to test that queries are built compatibly across versions. However, there are a number of problems with this approach:

- The tests have some gaps. For example, we can't always catch incompatibility from pushing up filters or reordering internal projections
- Tests are always incomplete, and breaking compatibility can lead to errors that are unrecoverable.
- The tests rely on an unstructured topology format taken from Kafka Streams. This is very brittle. Part of this proposal is to switch to a more structured format.
- The tests take a long time to run. Currently there are 473 test cases that must be repeated for every previous version of KSQL. This is going to grow as we support cloud, where we want to be able to release more frequently.
- The usage of compatibility-breaking configs adds a lot of complexity to the codebase.

In this one-pager, we propose building query execution plans at the SchemaKStream level that capture the low-level details of the computation being done at each step of a query. This plan can then be persisted to the command topic (instead of statements + config), and KSQL can then build queries from the persisted execution plans instead of rebuilding the query from scratch each time.

## What is in scope

- Generate a structured execution plan that describes all streams calls along with custom computations we do for KSQL.
- Persist the execution plan to the command topic and build queries from it.
- Augment the query-validation tests to persist execution plans as they change, while continuing to verify older plans.

## What is not in scope

- Exposing the execution plan to users. I think this is worthwhile, and a clear improvement over the string that's returned today, but it's not what we're trying to solve here.

## Value/Return

- Less risk during upgrades.
- Easier to maintain code.
- Can actually make breaking changes while still supporting upgrades (e.g. change the grammar, change the execution plan, internal optimizations, etc)

## Design

First let's try to describe what it means for topologies to be compatible:

- The two topologies should have the same set of sub-topologies.
- The sub-topologies must be compatible:
- The sub-topologies must have the same source Kafka topics (sources and state stores)
- The sub-topologies must perform the same computation on the data read from the source Kafka topics
- The sub-topologies must write the results out to the same Kafka topics (output and state stores) with the same formats.

In the following sections I'll describe short term and longer term solutions to ensuring compatibility.

### Issues With Current Tests

Currently, our approach to ensuring compatibility is to try and catch breaking changes with tests. However, these tests have some gaps. They verify that the Kafka Streams topologies match, and that the data written into and read from any Kafka topics is kept consistent (same schema, data format, and topic). However, they don't verify that the computation done by each step in the topology is kept consistent across versions (so we're not completely verifying step 2). This is because the Kakfa Streams topology doesn't describe the actual computation being done by a step beyond the name of the operator (mapValues, filter, etc).

To illustrate this, consider the following examples where we break compatibility in a manner that wouldn't be caught by the tests. The examples rely on the following query:

```
CREATE STREAM FOO (X INT, Y INT, ...) ...;
CREATE TABLE AGG AS SELECT X, Y, COUNT(*) FROM FOO WHERE X=3 GROUP BY X, Y HAVING COUNT(*) > 10 AND Y > 5;
```

- Reordering Projections: When compiling the aggregation, KSQL creates an internal schema with internally-generated column names before repartitioning the stream. If the projection from the original column names to the internal names is not maintained across versions, we can corrupt the state of the query (since the rows in the repartition topic at the time of upgrade will use the old projection)
- Moving Filters: In the example query, we currently evaluate the filter on column Y after computing the aggregation result. A worthwhile optimization for this query (which is technically valid though not a good query) would be to filter before computing the aggregation. However this would be a compatibility breaking change because at the time of an upgrade the repartition topic would contain the unfiltered rows. 

#### Execution Plans
To address this, I propose we build our own representation of the topology (called an Execution Plan) that provides details about what the streams operators are actually doing. Then, we can persist this plan to the command topic in place of (or in addition to) the original KSQL statement. When executing queries, we can build the final streams topologies from the persisted Execution Plan instead of rebuilding the queries from statements every time. This way, we don't rely as much on compatibility verification, and don't need to worry about query compatibility in most of the engine code.

The Execution Plan is a DAG of Execution Steps. Each Execution Step represents a call to Kafka Streams, along with context about what the resulting Streams operator is doing. We can build the Execution Plan at the SchemaKStream layer.

For example, the call to SchemaKStream.select would look something like:

```
public SchemaKStream<K> select(
    final List<SelectExpression> selectExpressions,
    final QueryContext.Stacker contextStacker,
    final ProcessingLogContext processingLogContext) {
  final ExecutionStep step = new StreamSelect(this.executionStep, selectExpressions);
  return new SchemaKStream<>(
      step.build(),
      ksqlConfig,
      functionRegistry,
      contextStacker.getQueryContext(),
      step
  );
}
```

Where StreamSelect looks like:

```
public class StreamSelect implements ExecutionStep {
  ...
  private final ExecutionStep source;
  private final List<SelectExpression> selectExpressions;

  @JsonCreator
  public StreamSelect(
      ...
      @JsonProperty("source") final ExecutionStep source,
      @JsonProperty("selectExpressions") final List<SelectExpression> selectExpressions) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.selectExpressions = ImmutableList.copyOf(selectExpressions);
  }

  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  @Override
  public List<ExecutionStep> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public KStream build(final KStream input) {
      final Selection selection = new Selection(selectExpressions);
      return kstream.mapValues(selection.getSelectValueMapper()),
  }
}
```

I've written up a basic implementation of Execution Plans which includes all the Execution Steps we'd need, and can be found here (https://github.com/confluentinc/ksql/tree/ksql-execution-plans/) (steps are under ksql-engine/src/main/java/io/confluent/ksql/structured/execution):

#### Serializability
The Execution Plan needs to be JSON-serializable. This way, we can persist the plan generated for each query into the command topic.

#### Versioning
We may need to version Execution Steps. Remember that a step is meant to represent a basic stream processing operation performed by a call to Kafka Streams. We may want to evolve how we perform a given operation in a way that is incompatible across versions. If a developer needs to modify a step in an incompatible way, they can do so by writing a new step type and advancing the version.

#### Splitting the Engine

This change effectively splits the current engine into two layers:

The upper layer builds Execution Plans, and consists of:

- DDL evaluation
- Parser
- Analyzer
- Logical Plan Builder
- Physical Plan Builder

Components in the upper layer would no longer need to worry about query compatibility.

The lower layer converts an Execution Plan into a Kafka Streams app. This layer consists of:

- Execution Steps
- Codegen
- Functions
- Serdes
- Expression parser

The lower layer does need to ensure its behavior is kept compatible with previous versions. However, the components at this layer don't have very complex interactions, so it should be reasonable to test for this at the unit level (which is what's being done today).

This layer should be split out from the rest of the engine into it's own package.

#### Existing Command Topic
Unless this change is introduced in a major version bump across which we choose to not support upgrades (which doesn't have to be the case for major bumps), we'll still need to support executing commands that don't have execution plans. However, this should be straightforward to do - we just build the plan when executing the command topic as we have always done.

#### Headless Mode
We need somewhere to store execution plans for statements in headless mode. Headless mode has no command topic. However we do have an internal topic used to store configs. We can add a new message type to this internal topic that stores a mapping from query ID to execution plan.

### Testing

#### Query Translation Tests
The functional changes proposed above ensure that we always build the same execution plan for a query. However, they don't quite ensure that the plan always performs the same operations. Unit tests are also brittle in this regard, as they are easily changed by a developer. To address this, we (courtesy of @big-andy-coates) propose to augment the query translation tests (QTT).

A QTT starts a query, and verifies that the computed output records given a reference set (list of records saved in git) of input records match a reference set of output records.

We will extend this to verify the contents of internal topics as well.

Furthermore, we will extend the test to verify the outputs and internal topic contents for execution plans (as opposed to just queries). For each test case (query) we'll persist all previously computed execution plans and expected internal and output reference sets, and verify each persisted plan as part of the test.

Finally, we'll include a tool to be run at release time that, for a given test case (query), will check whether the current engine generates a different plan from the latest persisted plan. If so, it will persist the plan computed by the current engine along with the current input, output, and internal reference sets.
