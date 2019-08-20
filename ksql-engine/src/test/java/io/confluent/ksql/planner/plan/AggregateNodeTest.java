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

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.LegacyFieldMatchers.hasName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;
import static org.hamcrest.CoreMatchers.containsString;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.LimitedProxyBuilder;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.QueryLoggerUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateNodeTest {

  private static final FunctionRegistry FUNCTION_REGISTRY = new InternalFunctionRegistry();
  private static final KsqlConfig KSQL_CONFIG =  new KsqlConfig(new HashMap<>());

  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private KeySerde<Struct> reboundKeySerde;
  @Mock
  private KeySerde<Windowed<Struct>> windowedKeySerde;
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;

  private StreamsBuilder builder = new StreamsBuilder();
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private final QueryId queryId = new QueryId("queryid");

  @Before
  public void setUp()  {
    when(keySerde.rebind(any(WindowInfo.class))).thenReturn(windowedKeySerde);
    when(reboundKeySerde.rebind(any(WindowInfo.class))).thenReturn(windowedKeySerde);
    when(keySerde.rebind(any(PersistenceSchema.class))).thenReturn(reboundKeySerde);
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test1")));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseConsistentOrderInPreAggSelectMapper() {
    // Given:
    final StreamBuilderMocker mocker = new StreamBuilderMocker();
    builder = mocker.createMockStreamBuilder();

    // When:
    buildQuery("SELECT col0, col1, col2, sum(col3), count(col3) FROM test1 "
        + "GROUP BY col0,col1,col2;");

    // Then:
    final List<ValueMapper> valueMappers = mocker.collectValueMappers();
    assertThat("invalid test", valueMappers, hasSize(greaterThanOrEqualTo(1)));
    final ValueMapper preAggSelectMapper = valueMappers.get(0);
    final GenericRow result = (GenericRow) preAggSelectMapper
        .apply(new GenericRow("rowtime", "rowkey", "0", "1", "2", "3"));
    assertThat("should select col0, col1, col2, col3", result.getColumns(),
        contains(0L, "1", "2", 3.0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseConsistentOrderInPostAggSelectMapper() {
    // Given:
    final StreamBuilderMocker mocker = new StreamBuilderMocker();
    builder = mocker.createMockStreamBuilder();

    // When:
    buildQuery("SELECT col0, sum(col3), count(col3), max(col3) FROM test1 GROUP BY col0;");

    // Then:
    final List<ValueMapper> valueMappers = mocker.collectValueMappers();
    assertThat("invalid test", valueMappers, hasSize(greaterThanOrEqualTo(2)));
    final ValueMapper postAggSelect = valueMappers.get(1);
    final GenericRow result = (GenericRow) postAggSelect
        .apply(new GenericRow("0", "-1", "2", "3", "4"));
    assertThat("should select col0, agg1, agg2", result.getColumns(), contains(0L, 2.0, 3L, 4.0));
  }

  @Test
  public void shouldHaveOneSubTopologyIfGroupByKey() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(1));
  }

  @Test
  public void shouldHaveTwoSubTopologies() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(2));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithLegacyNameForRepartition() {
    // When:
    buildRequireRekey(
        KSQL_CONFIG.overrideBreakingConfigsWithOriginalValues(
            ImmutableMap.of(
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                String.valueOf(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF))));

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000010");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000007")));
    assertThat(
        node.topicSet(),
        hasItem(equalTo("KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition")));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithKsqlNameForRepartition() {
    // When:
    buildRequireRekey();

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000006")));
    assertThat(node.topicSet(), containsInAnyOrder("Aggregate-groupby-repartition"));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithDefaultNameForRepartition() {
    buildRequireRekey(
        new KsqlConfig(
            ImmutableMap.of(
                StreamsConfig.TOPOLOGY_OPTIMIZATION,
                StreamsConfig.NO_OPTIMIZATION,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
        )
    );
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(),
        "KSTREAM-SOURCE-0000000010");
    final List<String> successors = node.successors().stream()
        .map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000007")));
    assertThat(
        node.topicSet(),
        hasItem(containsString("KSTREAM-AGGREGATE-STATE-STORE-0000000006")));
    assertThat(node.topicSet(), hasItem(containsString("-repartition")));
  }

  @Test
  public void shouldHaveDefaultNameForAggregationStateStoreIfInternalTopicNamingOff() {
    build(
        new KsqlConfig(
            ImmutableMap.of(
                StreamsConfig.TOPOLOGY_OPTIMIZATION,
                StreamsConfig.NO_OPTIMIZATION,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
        )
    );
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        builder.build(), "KSTREAM-AGGREGATE-0000000006");
    assertThat(node.stores(), hasItem(equalTo("KSTREAM-AGGREGATE-STATE-STORE-0000000005")));
  }

  @Test
  public void shouldHaveKsqlNameForAggregationStateStore() {
    build();
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        builder.build(), "KSTREAM-AGGREGATE-0000000005");
    assertThat(node.stores(), hasItem(equalTo("Aggregate-aggregate")));
  }

  @Test
  public void shouldHaveSinkNodeWithSameTopicAsSecondSource() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1;");

    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), "KSTREAM-SINK-0000000007");
    final TopologyDescription.Source source = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(source.topicSet(), hasItem(sink.topic()));
  }

  @Test
  public void shouldBuildCorrectAggregateSchema() {
    // When:
    final SchemaKStream stream = buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    assertThat(stream.getSchema().valueFields(), contains(
        Field.of("COL0", SqlTypes.BIGINT),
        Field.of("KSQL_COL_1", SqlTypes.DOUBLE),
        Field.of("KSQL_COL_2", SqlTypes.BIGINT)));
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
        + "WHERE col0 > 100 GROUP BY col0;", ksqlConfig);
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
        + "GROUP BY col1;", ksqlConfig);
  }

  @Test
  public void shouldCreateLoggers() {
    // When:
    final AggregateNode node = buildAggregateNode(
        "SELECT col0, sum(col3), count(col3) FROM test1 GROUP BY col0;");
    buildQuery(node, KSQL_CONFIG);

    // Then:
    verify(ksqlStreamBuilder, times(3)).buildValueSerde(
        any(),
        any(),
        queryContextCaptor.capture()
    );

    final List<String> loggers = queryContextCaptor.getAllValues().stream()
        .map(QueryLoggerUtil::queryLoggerName)
        .collect(Collectors.toList());

    assertThat(loggers, contains(
        "queryid.KsqlTopic.source",
        "queryid.Aggregate.groupby",
        "queryid.Aggregate.aggregate"
    ));
  }

  @Test
  public void shouldGroupByFunction() {
    // Given:
    final SchemaKStream<?> stream = buildQuery("SELECT UCASE(col1), sum(col3), count(col3) FROM test1 "
        + "GROUP BY UCASE(col1);");

    // Then:
    assertThat(stream.getKeyField().name(), is(Optional.empty()));
    assertThat(stream.getKeyField().legacy(), OptionalMatchers.of(hasName("UCASE(KSQL_INTERNAL_COL_0)")));
  }

  @Test
  public void shouldGroupByArithmetic() {
    // Given:
    final SchemaKStream<?> stream = buildQuery("SELECT col0 + 10, sum(col3), count(col3) FROM test1 "
        + "GROUP BY col0 + 10;");

    // Then:
    assertThat(stream.getKeyField().name(), is(Optional.empty()));
    assertThat(stream.getKeyField().legacy(), OptionalMatchers.of(hasName("(KSQL_INTERNAL_COL_0 + 10)")));
  }

  private SchemaKStream buildQuery(final String queryString) {
    return buildQuery(queryString, KSQL_CONFIG);
  }

  private SchemaKStream buildQuery(final String queryString, final KsqlConfig ksqlConfig) {
    return buildQuery(buildAggregateNode(queryString), ksqlConfig);
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream buildQuery(final AggregateNode aggregateNode, final KsqlConfig ksqlConfig) {
    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(ksqlStreamBuilder.getFunctionRegistry()).thenReturn(FUNCTION_REGISTRY);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(queryId)
            .push(inv.getArgument(0).toString()));
    when(ksqlStreamBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);

    return aggregateNode.buildStream(ksqlStreamBuilder);
  }

  private static AggregateNode buildAggregateNode(final String queryString) {
    final MetaStore newMetaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
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
                  .flatMap(t -> t.mapValues.keySet().stream())
          )).collect(Collectors.toList());
    }

    private static final class FakeKStream {

      private final Map<ValueMapper, FakeKStream> mapValues = new IdentityHashMap<>();
      private final Map<ValueMapperWithKey, FakeKStream> mapValuesWithKey = new IdentityHashMap<>();
      private final Map<ValueTransformerSupplier, FakeKStream> transformValues = new IdentityHashMap<>();
      private final Map<Predicate, FakeKStream> filter = new IdentityHashMap<>();
      private final Map<Grouped, FakeKGroupedStream> groupByKey = new IdentityHashMap<>();

      KStream createProxy() {
        return LimitedProxyBuilder.forClass(KStream.class)
            .forward("mapValues", methodParams(ValueMapper.class), this)
            .forward("mapValues", methodParams(ValueMapperWithKey.class), this)
            .forward("transformValues",
                methodParams(ValueTransformerSupplier.class, String[].class), this)
            .forward("filter", methodParams(Predicate.class), this)
            .forward("groupByKey", methodParams(Grouped.class), this)
            .forward("groupBy", methodParams(KeyValueMapper.class, Grouped.class), this)
            .build();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KStream mapValues(final ValueMapper mapper) {
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
      private KStream transformValues(
          final ValueTransformerSupplier valueTransformerSupplier,
          final String... stateStoreNames
      ) {
        final FakeKStream stream = new FakeKStream();
        transformValues.put(valueTransformerSupplier, stream);
        return stream.createProxy();
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
            transformValues.values().stream()
        );
        final Stream<FakeKStream> grandChildren = Streams.concat(
            mapValues.values().stream(),
            mapValuesWithKey.values().stream(),
            filter.values().stream(),
            transformValues.values().stream()
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

      KTable createProxy() {
        return LimitedProxyBuilder.forClass(KTable.class)
            .forward("mapValues", methodParams(ValueMapper.class), this)
            .build();
      }

      @SuppressWarnings("unused") // Invoked via reflection.
      private KTable mapValues(final ValueMapper mapper) {
        final FakeKTable table = new FakeKTable();
        mapValues.put(mapper, table);
        return table.createProxy();
      }
    }
  }
}
