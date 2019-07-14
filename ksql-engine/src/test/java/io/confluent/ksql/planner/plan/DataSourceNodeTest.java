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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceNodeTest {

  private static final String TIMESTAMP_FIELD = "timestamp";
  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");

  private final KsqlConfig realConfig = new KsqlConfig(Collections.emptyMap());
  private SchemaKStream realStream;
  private StreamsBuilder realBuilder;
  private final LogicalSchema realSchema = LogicalSchema.of(SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field3", Schema.OPTIONAL_STRING_SCHEMA)
      .field(TIMESTAMP_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private final KsqlStream<String> SOME_SOURCE = new KsqlStream<>(
      "sqlExpression",
      "datasource",
      realSchema,
      SerdeOption.none(),
      KeyField.of("key", realSchema.valueSchema().field("key")),
      new LongColumnTimestampExtractionPolicy("timestamp"),
      new KsqlTopic("topic", "topic",
          new KsqlJsonSerdeFactory(), false),
      Serdes::String
  );

  private final DataSourceNode node = new DataSourceNode(
      PLAN_NODE_ID,
      SOME_SOURCE,
      SOME_SOURCE.getName());

  private final QueryId queryId = new QueryId("source-test");

  private final PlanNodeId realNodeId = new PlanNodeId("source");
  @Mock
  private KsqlTable tableSource;
  @Mock
  private TimestampExtractionPolicy timestampExtractionPolicy;
  @Mock
  private TimestampExtractor timestampExtractor;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private KsqlSerdeFactory valueSerDeFactory;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private Serde<String> keySerde;
  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private KStream<?, ?> kStream;
  @Mock
  private KGroupedStream kGroupedStream;
  @Mock
  private KTable kTable;
  @Mock
  private Function<KsqlConfig, MaterializedFactory> materializedFactorySupplier;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized materialized;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;

  private final Set<SerdeOption> serdeOptions = SerdeOption.none();

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    realBuilder = new StreamsBuilder();

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(realConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(realBuilder);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(queryId)
            .push(inv.getArgument(0).toString()));
    when(ksqlStreamBuilder.buildGenericRowSerde(any(), any(), any())).thenReturn(rowSerde);
    when(ksqlStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);

    when(rowSerde.serializer()).thenReturn(mock(Serializer.class));
    when(rowSerde.deserializer()).thenReturn(mock(Deserializer.class));

    when(tableSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(tableSource.isWindowed()).thenReturn(false);
    when(tableSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(tableSource.getKeySerdeFactory()).thenReturn(() -> keySerde);
    when(tableSource.getTimestampExtractionPolicy()).thenReturn(timestampExtractionPolicy);
    when(tableSource.getSerdeOptions()).thenReturn(serdeOptions);
    when(ksqlTopic.getKafkaTopicName()).thenReturn("topic");
    when(ksqlTopic.getValueSerdeFactory()).thenReturn(valueSerDeFactory);
    when(timestampExtractionPolicy.timestampField()).thenReturn(TIMESTAMP_FIELD);
    when(timestampExtractionPolicy.create(anyInt())).thenReturn(timestampExtractor);
    when(kStream.transformValues(any(ValueTransformerSupplier.class))).thenReturn(kStream);
    when(kStream.mapValues(any(ValueMapperWithKey.class))).thenReturn(kStream);
    when(kStream.mapValues(any(ValueMapper.class))).thenReturn(kStream);
    when(kStream.groupByKey()).thenReturn(kGroupedStream);
    when(kGroupedStream.aggregate(
        any(Initializer.class),
        any(Aggregator.class),
        any(Materialized.class))).thenReturn(kTable);
    when(materializedFactorySupplier.apply(any(KsqlConfig.class)))
        .thenReturn(materializedFactory);
    when(materializedFactory.create(any(Serde.class), any(Serde.class), anyString()))
        .thenReturn(materialized);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldMaterializeTableCorrectly() {
    // Given:
    final DataSourceNode node = nodeWithMockTableSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(materializedFactorySupplier).apply(realConfig);
    verify(materializedFactory).create(keySerde, rowSerde, "source-reduce");
    verify(kGroupedStream).aggregate(any(), any(), same(materialized));
  }

  @Test
  public void shouldCreateLoggerForSourceSerde() {
    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(ksqlStreamBuilder).buildGenericRowSerde(
        any(),
        any(),
        queryContextCaptor.capture()
    );

    assertThat(QueryLoggerUtil.queryLoggerName(queryContextCaptor.getValue()),
        is("source-test.0.source"));
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    realStream = node.buildStream(ksqlStreamBuilder);

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(realBuilder.build(), PlanTestUtil.SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(PlanTestUtil.MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("topic")));
  }

  @Test
  public void shouldBuildMapNode() {
    // When:
    realStream = node.buildStream(ksqlStreamBuilder);

    // Then:
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(realBuilder.build(), PlanTestUtil.MAPVALUES_NODE),
        Collections.singletonList(PlanTestUtil.SOURCE_NODE),
        Collections.singletonList(PlanTestUtil.TRANSFORM_NODE));
  }

  @Test
  public void shouldBuildTransformNode() {
    // When:
    realStream = node.buildStream(ksqlStreamBuilder);

    // Then:
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        realBuilder.build(), PlanTestUtil.TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(PlanTestUtil.MAPVALUES_NODE), Collections.emptyList());
  }

  @Test
  public void shouldBeOfTypeSchemaKStreamWhenDataSourceIsKsqlStream() {
    // When:
    realStream = node.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(realStream.getClass(), equalTo(SchemaKStream.class));
  }

  @Test
  public void shouldBuildStreamWithSameKeyField() {
    // When:
    final SchemaKStream<?> stream = node.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(stream.getKeyField(), is(node.getKeyField()));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    final KsqlTable<String> table = new KsqlTable<>("sqlExpression", "datasource",
        realSchema,
        SerdeOption.none(),
        KeyField.of("field1", realSchema.valueSchema().field("field1")),
        new LongColumnTimestampExtractionPolicy("timestamp"),
        new KsqlTopic("topic2", "topic2",
            new KsqlJsonSerdeFactory(), false),
        Serdes::String
    );

    final DataSourceNode node = new DataSourceNode(
        PLAN_NODE_ID,
        table,
        table.getName());

    final SchemaKStream result = node.buildStream(ksqlStreamBuilder);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldTransformKStreamToKTableCorrectly() {
    final KsqlTable<String> table = new KsqlTable<>("sqlExpression", "datasource",
        realSchema,
        SerdeOption.none(),
        KeyField.of("field1", realSchema.valueSchema().field("field1")),
        new LongColumnTimestampExtractionPolicy("timestamp"),
        new KsqlTopic("topic2", "topic2",
            new KsqlJsonSerdeFactory(), false),
        Serdes::String
    );

    final DataSourceNode node = new DataSourceNode(
        PLAN_NODE_ID,
        table,
        table.getName());

    realBuilder = new StreamsBuilder();
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(realBuilder);
    node.buildStream(ksqlStreamBuilder);
    final Topology topology = realBuilder.build();
    final TopologyDescription description = topology.describe();

    final List<String> expectedPlan = Arrays.asList(
        "SOURCE", "MAPVALUES", "TRANSFORMVALUES", "MAPVALUES", "AGGREGATE");

    assertThat(description.subtopologies().size(), equalTo(1));
    final Set<TopologyDescription.Node> nodes = description.subtopologies().iterator().next().nodes();
    // Get the source node
    TopologyDescription.Node streamsNode = nodes.iterator().next();
    while (!streamsNode.predecessors().isEmpty()) {
      streamsNode = streamsNode.predecessors().iterator().next();
    }
    // Walk the plan and make sure it matches
    final ListIterator<String> expectedPlanIt = expectedPlan.listIterator();
    assertThat(nodes.size(), equalTo(expectedPlan.size()));
    while (true) {
      assertThat(streamsNode.name(), startsWith("KSTREAM-" + expectedPlanIt.next()));
      if (streamsNode.successors().isEmpty()) {
        assertThat(expectedPlanIt.hasNext(), is(false));
        break;
      }
      assertThat(expectedPlanIt.hasNext(), is(true));
      assertThat(streamsNode.successors().size(), equalTo(1));
      streamsNode = streamsNode.successors().iterator().next();
    }
  }

  @Test
  public void shouldHaveFullyQualifiedSchema() {
    // Given:
    final String sourceName = SOME_SOURCE.getName();

    // When:
    final LogicalSchema schema = node.getSchema();

    // Then:
    assertThat(schema, is(
        LogicalSchema.of(SchemaBuilder.struct()
            .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
            .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
            .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
            .field("field3", Schema.OPTIONAL_STRING_SCHEMA)
            .field(TIMESTAMP_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
            .field("key", Schema.OPTIONAL_STRING_SCHEMA)
            .build()).withAlias(sourceName)));
  }

  @Test
  public void shouldBuildRowSerdeWithSourceSchema() {
    // Given:
    final DataSourceNode node = nodeWithMockTableSource();

    final PhysicalSchema expected = PhysicalSchema
        .from(realSchema.withoutMetaAndKeyFieldsInValue(), serdeOptions);

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(ksqlStreamBuilder).buildGenericRowSerde(
        any(),
        eq(expected),
        any());
  }

  @SuppressWarnings("unchecked")
  private DataSourceNode nodeWithMockTableSource() {
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(streamsBuilder.stream(anyString(), any())).thenReturn((KStream)kStream);
    when(tableSource.getSchema()).thenReturn(realSchema);
    when(tableSource.getKeyField())
        .thenReturn(KeyField.of("field1", realSchema.valueSchema().field("field1")));

    return new DataSourceNode(
        realNodeId,
        tableSource,
        "t",
        materializedFactorySupplier);
  }
}
