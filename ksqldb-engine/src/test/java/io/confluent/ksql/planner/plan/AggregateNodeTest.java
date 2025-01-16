/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.function.UserFunctionLoaderTestUtil.loadAllUserFunctions;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.LimitedProxyBuilder;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"UnstableApiUsage", "unchecked"})
@RunWith(MockitoJUnitRunner.class)
public class AggregateNodeTest {

  private static final MutableFunctionRegistry FUNCTION_REGISTRY = new InternalFunctionRegistry();
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(new HashMap<>());

  @Mock
  private PlanBuildContext buildContext;
  @Mock
  private RuntimeBuildContext runtimeBuildContext;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private ProcessorContext ctx;
  @Mock
  private FixedKeyProcessorContext fixedKeyProcessorContext;
  @Mock
  private ProcessingLogger processLogger;
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;

  private StreamsBuilder builder = new StreamsBuilder();
  private final QueryId queryId = new QueryId("queryid");

  @BeforeClass
  public static void setUpFunctionRegistry() {
    loadAllUserFunctions(FUNCTION_REGISTRY);
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;");

    // Then:
    final TopologyDescription.Source sourceNode = (TopologyDescription.Source)
        getNodeByName(builder.build(), SOURCE_NODE);

    final List<String> successors = sourceNode.successors().stream()
        .map(TopologyDescription.Node::name)
        .collect(Collectors.toList());

    assertThat(sourceNode.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(TRANSFORM_NODE)));
    assertThat(sourceNode.topicSet(), equalTo(ImmutableSet.of("test1")));
  }

  /**
   * Aggregation requires 3 node/operator in the topology:
   *  1. Source node (SourceBuilderUtils.AddKeyAndPseudoColumns(KeyGenerator: SourceBuilderV1)
   *  2. Pre-Agg Select Mapper - Picks the columns needed for grouping and aggregate functions
   *    (StreamSelectBuilder with SelectValueMapper)
   *  3. Post-Agg Select Mapper - Projects the output columns
   *    (TableSelectBuilder with SelectValueMapper)
   *  This test checks:
   *  1. the pre-agg inclusion (hence >= 2).
   *  2. the pre-agg supplier working correctly.
   */
  @Test
  @SuppressWarnings("unchecked") // For generic type casting
  public void shouldUseConsistentOrderInPreAggSelectMapper() {
    // Given:
    final StreamBuilderMocker mocker = new StreamBuilderMocker();
    builder = mocker.createMockStreamBuilder();

    // When:
    buildQuery("SELECT col0, col1, col2, sum(col3), count(col3) FROM test1 "
        + "GROUP BY col0,col1,col2 EMIT CHANGES;"); // should select col0, col1, col2, col3;

    // Then:
    final List<Supplier> processorSuppliers = mocker
        .collectProcessorSuppliers();

    assertThat("invalid test", processorSuppliers, hasSize(greaterThanOrEqualTo(2)));
    // pre-agg processor
    final FixedKeyProcessor preAggProcessor = (FixedKeyProcessor) processorSuppliers.get(1).get();
    preAggProcessor.init(fixedKeyProcessorContext);
    final FixedKeyRecord record = mock(FixedKeyRecord.class);
    when(record.value())
        .thenReturn(genericRow("1", "2", 3.0D, null, null,
            "headers", "rowtime", "rowpartition", "rowoffset", 0L));
    // mock withValue to return new mock with new value
    when(record.withValue(any())).thenAnswer(inv -> {
      final GenericRow row = inv.getArgument(0);
      final FixedKeyRecord newRecord = mock(FixedKeyRecord.class);
      when(newRecord.value()).thenReturn(row);
      return newRecord;
    });
    preAggProcessor.process(record);
    verify(fixedKeyProcessorContext).forward(argThat(
        r -> {
          assertThat("should select col0, col1, col2, col3",
              ((GenericRow) r.value()).values(), contains(0L, "1", "2", 3.0));
          return true;
        }
        ));
  }

  /**
   * refer to {@link #shouldUseConsistentOrderInPreAggSelectMapper()} for details.
   * This test checks
   * 1. the post-agg inclusion (hence >= 2)
   * 2. the post-agg supplier working correctly.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseConsistentOrderInPostAggSelectMapper() {
    // Given:
    final StreamBuilderMocker mocker = new StreamBuilderMocker();
    builder = mocker.createMockStreamBuilder();

    // When:
    buildQuery("SELECT col0, sum(col3), count(col3), max(col3) FROM test1 "
        + "GROUP BY col0 EMIT CHANGES;"); // pre-agg: col0, col3; post-agg: col0, agg1, agg2, agg3

    // Then:
    final List<Supplier> suppliers = mocker
        .collectProcessorSuppliers();

    assertThat("invalid test",
        suppliers.size(), greaterThanOrEqualTo(3));
    // Filter suppliers to get ValueTransformerWithKeySupplier
    final KsValueTransformer<Long, GenericRow> postAggTransformer
        = (KsValueTransformer<Long, GenericRow>) suppliers.get(2).get();
    postAggTransformer.init(ctx);
    final GenericRow result =
        postAggTransformer.transform(null,
            genericRow(0L, "-1", 2.0D, 3L, 4.0D));
    assertThat("should select col0, agg1, agg2 agg3", result.values(), contains(0L, 2.0, 3L, 4.0));
  }

  @Test
  public void shouldHaveOneSubTopologyIfGroupByKey() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(1));
  }

  @Test
  public void shouldHaveTwoSubTopologies() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1 EMIT CHANGES;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(2));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubTopologyWithKsqlNameForRepartition() {
    // When:
    buildRequireRekey();

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(), "Aggregate-GroupBy-repartition-source");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000005")));
    assertThat(node.topicSet(), containsInAnyOrder("Aggregate-GroupBy-repartition"));
  }

  /**
   * Processor Api is used for the aggregate step, so the topology will look like:
   * Source (0000) ->
   *  Transform (0001) ->
   *    Filter ->
   *      Process (0002) ->  // Combined some transforms into process
   *        Aggregate (0003)   // Number reduced as compared to transformer
   */
  @Test
  public void shouldHaveKsqlNameForAggregationStateStore() {
    build();
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        builder.build(), "KSTREAM-AGGREGATE-0000000004");
    assertThat(node.stores(), hasItem(equalTo("Aggregate-Aggregate-Materialize")));
  }

  @Test
  public void shouldHaveSinkNodeWithSameTopicAsSecondSource() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1 EMIT CHANGES;");

    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(),
        "Aggregate-GroupBy-repartition-sink");
    final TopologyDescription.Source source = (TopologyDescription.Source) getNodeByName(
        builder.build(), "Aggregate-GroupBy-repartition-source");
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(source.topicSet(), hasItem(sink.topic()));
  }

  @Test
  public void shouldBuildCorrectAggregateSchema() {
    // When:
    final SchemaKStream<?> stream = buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;");

    // Then:
    assertThat(stream.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("KSQL_COL_1"), SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldBuildCorrectMultiArgAggregateSchema() {
    // When:
    final SchemaKStream<?> stream = buildQuery("SELECT col0, multi_arg(col0, col1, 20) FROM test1 "
            + "window TUMBLING (size 2 second) "
            + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;");

    // Then:
    assertThat(stream.getSchema(), is(LogicalSchema.builder()
            .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.BIGINT)
            .build()
    ));
  }

  @Test
  public void shouldBuildCorrectVarArgAggregateSchema() {
    // When:
    final SchemaKStream<?> stream = buildQuery("SELECT col0, var_arg(col0, col1, col2) FROM test1 "
            + "window TUMBLING (size 2 second) "
            + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;");

    // Then:
    assertThat(stream.getSchema(), is(LogicalSchema.builder()
            .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("KSQL_COL_0"), SqlTypes.BIGINT)
            .build()
    ));
  }

  @Test
  public void shouldBeSchemaKTableResult() {
    final SchemaKStream stream = build();
    assertThat(stream.getClass(), equalTo(SchemaKTable.class));
  }

  private SchemaKStream build() {
    return build(KSQL_CONFIG);
  }

  private SchemaKStream build(final KsqlConfig ksqlConfig) {
    return buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;", ksqlConfig);
  }

  @SuppressWarnings("UnusedReturnValue")
  private SchemaKStream buildRequireRekey() {
    return buildRequireRekey(KSQL_CONFIG);
  }

  @SuppressWarnings("UnusedReturnValue")
  private SchemaKStream buildRequireRekey(final KsqlConfig ksqlConfig) {
    return buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "GROUP BY col1 EMIT CHANGES;", ksqlConfig);
  }

  @Test
  public void shouldCreateLoggers() {
    // When:
    final AggregateNode node = buildAggregateNode(
        "SELECT col0, sum(col3), count(col3) FROM test1 GROUP BY col0 EMIT CHANGES;");
    buildQuery(node, KSQL_CONFIG);

    // Then:
    verify(runtimeBuildContext, times(4)).buildValueSerde(
        any(),
        any(),
        queryContextCaptor.capture()
    );

    final List<String> loggers = queryContextCaptor.getAllValues().stream()
        .map(ctx -> QueryLoggerUtil.queryLoggerName(queryId, ctx))
        .collect(Collectors.toList());

    assertThat(loggers, contains(
        "queryid.KsqlTopic.Source",
        "queryid.Aggregate.GroupBy",
        "queryid.Aggregate.Aggregate.Materialize",
        "queryid.Aggregate.Project"
    ));
  }

  private SchemaKStream buildQuery(final String queryString) {
    return buildQuery(queryString, KSQL_CONFIG);
  }

  private SchemaKStream buildQuery(final String queryString, final KsqlConfig ksqlConfig) {
    return buildQuery(buildAggregateNode(queryString), ksqlConfig);
  }

  private SchemaKStream buildQuery(final AggregateNode aggregateNode, final KsqlConfig ksqlConfig) {
    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(FUNCTION_REGISTRY);
    when(buildContext.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));
    when(runtimeBuildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(runtimeBuildContext.getFunctionRegistry()).thenReturn(FUNCTION_REGISTRY);
    when(runtimeBuildContext.getStreamsBuilder()).thenReturn(builder);
    when(runtimeBuildContext.getProcessingLogger(any())).thenReturn(processLogger);
    when(runtimeBuildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);

    final SchemaKTable schemaKTable = (SchemaKTable) aggregateNode.buildStream(buildContext);
    schemaKTable.getSourceTableStep().build(new KSPlanBuilder(runtimeBuildContext));
    return schemaKTable;
  }

  private static AggregateNode buildAggregateNode(final String queryString) {
    final MetaStore newMetaStore = MetaStoreFixture.getNewMetaStore(FUNCTION_REGISTRY);
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) AnalysisTestUtil
        .buildLogicalPlan(KSQL_CONFIG, queryString, newMetaStore);

    return (AggregateNode) planNode.getSource();
  }

  private static final class StreamBuilderMocker {

    private final Map<String, FakeKStream> sources = new HashMap<>();

    private StreamsBuilder createMockStreamBuilder() {
      final StreamsBuilder builder = mock(StreamsBuilder.class);
      when(builder.stream(anyString(), any())).thenAnswer(inv -> {
        final FakeKStream stream = new FakeKStream();
        sources.put(inv.getArgument(0), stream);
        return stream.createProxy();
      });
      return builder;
    }

    List<ValueMapper> collectValueMappers() {
      return sources.values().stream()
          .flatMap(stream -> Streams.concat(Stream.of(stream), stream.stream()))
          .flatMap(stream -> Streams.concat(
              stream.mapValues.keySet().stream(),
              stream.groupStreams()
                  .flatMap(FakeKGroupedStream::tables)
                  .flatMap(FakeKTable::tables)
                  .flatMap(t -> t.mapValues.keySet().stream())
          )).collect(Collectors.toList());
    }

    List<Supplier> collectProcessorSuppliers() {
      return sources.values().stream()
          .flatMap(stream -> Streams.concat(Stream.of(stream), stream.stream()))
          .flatMap(stream -> Streams.concat(
              stream.supplierStreamsMap.keySet().stream(), // supplier keys
              stream.groupStreams()
                  .flatMap(FakeKGroupedStream::tables)
                  .flatMap(FakeKTable::tables)
                  .flatMap(t -> t.transformValues.keySet().stream())
          )).collect(Collectors.toList());
    }

    private static final class FakeKStream {

      // Maps to hold the calling of the functions
      private final Map<ValueMapper, FakeKStream> mapValues = new IdentityHashMap<>();
      private final Map<ValueMapperWithKey, FakeKStream> mapValuesWithKey = new IdentityHashMap<>();
      private final Map<Supplier, FakeKStream> supplierStreamsMap
          = new IdentityHashMap<>();
      private final Map<Predicate, FakeKStream> filter = new IdentityHashMap<>();
      private final Map<Grouped, FakeKGroupedStream> groupByKey = new IdentityHashMap<>();

      KStream createProxy() {
        return LimitedProxyBuilder.forClass(KStream.class)
            .forward("mapValues", methodParams(ValueMapper.class), this)
            .forward("mapValues", methodParams(ValueMapper.class, Named.class),
                this)
            .forward("mapValues", methodParams(ValueMapperWithKey.class), this)
            .forward("mapValues", methodParams(ValueMapperWithKey.class, Named.class),
                this)
            .forward("process",
                methodParams(ProcessorSupplier.class, String[].class), this)
            .forward("process",
                methodParams(ProcessorSupplier.class, Named.class, String[].class), this)
            .forward("processValues",
                methodParams(FixedKeyProcessorSupplier.class, String[].class), this)
            .forward("processValues",
                methodParams(FixedKeyProcessorSupplier.class, Named.class, String[].class), this)
            .forward("filter", methodParams(Predicate.class), this)
            .forward("groupByKey", methodParams(Grouped.class), this)
            .forward("groupBy", methodParams(KeyValueMapper.class, Grouped.class), this)
            .forward("peek", methodParams(ForeachAction.class), this)
            .build();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream mapValues(final ValueMapper mapper) {
        final FakeKStream stream = new FakeKStream();
        mapValues.put(mapper, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream mapValues(final ValueMapper mapper, final Named named) {
        final FakeKStream stream = new FakeKStream();
        mapValues.put(mapper, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream mapValues(final ValueMapperWithKey mapper) {
        final FakeKStream stream = new FakeKStream();
        mapValuesWithKey.put(mapper, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream mapValues(final ValueMapperWithKey mapper, final Named named) {
        final FakeKStream stream = new FakeKStream();
        mapValuesWithKey.put(mapper, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream transformValues(
          final ValueTransformerWithKeySupplier valueTransformerSupplier,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(valueTransformerSupplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream transformValues(
          final ValueTransformerWithKeySupplier valueTransformerSupplier,
          final Named named,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(valueTransformerSupplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused")
      private KStream process(
          final ProcessorSupplier processorSupplier,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(processorSupplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused")
      private KStream process(
          final ProcessorSupplier processorSupplier,
          final Named named,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(processorSupplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused")
      private KStream processValues(
          final FixedKeyProcessorSupplier supplier,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(supplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused")
      private KStream processValues(
          final FixedKeyProcessorSupplier supplier,
          final Named named,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        supplierStreamsMap.put(supplier, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream peek(final ForeachAction action) {
        return new FakeKStream().createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream filter(final Predicate predicate) {
        final FakeKStream stream = new FakeKStream();
        filter.put(predicate, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KGroupedStream groupByKey(final Grouped grouped) {
        final FakeKGroupedStream stream = new FakeKGroupedStream();
        groupByKey.put(grouped, stream);
        return stream.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KGroupedStream groupBy(final KeyValueMapper selector, final Grouped grouped) {
        final FakeKGroupedStream stream = new FakeKGroupedStream();
        groupByKey.put(grouped, stream);
        return stream.createProxy();
      }

      Stream<FakeKStream> stream() {
        final Stream<FakeKStream> children = Streams.concat(
            mapValues.values().stream(),
            mapValuesWithKey.values().stream(),
            filter.values().stream(),
            supplierStreamsMap.values().stream(),
            mapValues.values().stream()
        );
        final Stream<FakeKStream> grandChildren = Streams.concat(
            mapValues.values().stream(),
            mapValuesWithKey.values().stream(),
            filter.values().stream(),
            supplierStreamsMap.values().stream(),
            mapValues.values().stream()
        ).flatMap(FakeKStream::stream);

        return Streams.concat(children, grandChildren);
      }

      Stream<FakeKGroupedStream> groupStreams() {
        return groupByKey.values().stream();
      }
    }

    private static final class FakeKGroupedStream {

      private final Map<Aggregator, FakeKTable> aggregate = new IdentityHashMap<>();

      KGroupedStream createProxy() {
        return LimitedProxyBuilder.forClass(KGroupedStream.class)
            .forward("aggregate",
                methodParams(Initializer.class, Aggregator.class, Materialized.class), this)
            .build();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KTable aggregate(
          final Initializer initializer,
          final Aggregator aggregator,
          final Materialized materialized
      ) {
        final FakeKTable table = new FakeKTable();
        aggregate.put(aggregator, table);
        return table.createProxy();
      }

      Stream<FakeKTable> tables() {
        return aggregate.values().stream();
      }
    }

    private static final class FakeKTable {

      private final Map<ValueMapper, FakeKTable> mapValues = new IdentityHashMap<>();
      private final Map<ValueTransformerWithKeySupplier, FakeKTable> transformValues = new IdentityHashMap<>();

      KTable createProxy() {
        return LimitedProxyBuilder.forClass(KTable.class)
            .forward("mapValues", methodParams(ValueMapper.class), this)
            .forward("transformValues",
                methodParams(ValueTransformerWithKeySupplier.class, Named.class, String[].class), this)
            .forward("transformValues",
                methodParams(ValueTransformerWithKeySupplier.class, Materialized.class, Named.class, String[].class), this)
            .build();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KTable mapValues(final ValueMapper mapper) {
        final FakeKTable table = new FakeKTable();
        mapValues.put(mapper, table);
        return table.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KTable transformValues(
          final ValueTransformerWithKeySupplier valueTransformerSupplier,
          final Named named,
          final String... stateStoreNames
      ) {
        final FakeKTable table = new FakeKTable();
        transformValues.put(valueTransformerSupplier, table);
        return table.createProxy();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KTable transformValues(
          final ValueTransformerWithKeySupplier valueTransformerSupplier,
          final Materialized materialized,
          final Named named,
          final String... stateStoreNames
      ) {
        final FakeKTable table = new FakeKTable();
        transformValues.put(valueTransformerSupplier, table);
        return table.createProxy();
      }

      Stream<FakeKTable> tables() {
        final Stream<FakeKTable> children = Streams.concat(
            mapValues.values().stream(),
            transformValues.values().stream()
        );
        final Stream<FakeKTable> grandChildren = Streams.concat(
            mapValues.values().stream(),
            transformValues.values().stream()
        ).flatMap(FakeKTable::tables);

        return Streams.concat(children, grandChildren);
      }
    }
  }
}
