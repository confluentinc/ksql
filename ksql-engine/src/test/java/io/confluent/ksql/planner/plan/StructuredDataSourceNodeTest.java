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

import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
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
public class StructuredDataSourceNodeTest {

  private static final String TIMESTAMP_FIELD = "timestamp";
  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");

  private final KsqlConfig realConfig = new KsqlConfig(Collections.emptyMap());
  private SchemaKStream realStream;
  private StreamsBuilder realBuilder;
  private final Schema realSchema = SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field3", Schema.OPTIONAL_STRING_SCHEMA)
      .field(TIMESTAMP_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private final KsqlStream<String> SOME_SOURCE = new KsqlStream<>(
      "sqlExpression",
      "datasource",
      realSchema,
      KeyField.of("key", realSchema.field("key")),
      new LongColumnTimestampExtractionPolicy("timestamp"),
      new KsqlTopic("topic", "topic",
          new KsqlJsonTopicSerDe(), false), Serdes::String);

  private final StructuredDataSourceNode node = new StructuredDataSourceNode(
      PLAN_NODE_ID,
      SOME_SOURCE,
      realSchema,
      KeyField.of("field1", realSchema.field("field1")));

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
  private KsqlTopicSerDe topicSerDe;
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
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    realBuilder = new StreamsBuilder();

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(realConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(realBuilder);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(queryId)
            .push(inv.getArgument(0).toString()));

    realStream = node.buildStream(ksqlStreamBuilder);

    when(tableSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(tableSource.isWindowed()).thenReturn(false);
    when(tableSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(tableSource.getKeySerdeFactory()).thenReturn(() -> keySerde);
    when(tableSource.getTimestampExtractionPolicy()).thenReturn(timestampExtractionPolicy);
    when(ksqlTopic.getKafkaTopicName()).thenReturn("topic");
    when(ksqlTopic.getKsqlTopicSerDe()).thenReturn(topicSerDe);
    when(ksqlStreamBuilder.buildGenericRowSerde(any(), any(), any())).thenReturn(rowSerde);
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
    final StructuredDataSourceNode node = nodeWithMockTableSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(materializedFactorySupplier).apply(realConfig);
    verify(materializedFactory).create(keySerde, rowSerde, "source-reduce");
    verify(kGroupedStream).aggregate(any(), any(), same(materialized));
  }

  @Test
  public void shouldCreateLoggerForSourceSerde() {
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
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(realBuilder.build(), PlanTestUtil.SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(PlanTestUtil.MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("topic")));
  }

  @Test
  public void shouldBuildMapNode() {
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(realBuilder.build(), PlanTestUtil.MAPVALUES_NODE),
        Collections.singletonList(PlanTestUtil.SOURCE_NODE),
        Collections.singletonList(PlanTestUtil.TRANSFORM_NODE));
  }

  @Test
  public void shouldBuildTransformNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        realBuilder.build(), PlanTestUtil.TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(PlanTestUtil.MAPVALUES_NODE), Collections.emptyList());
  }

  @Test
  public void shouldHaveNoOutputNode() {
    assertThat(realStream.outputNode(), nullValue());
  }

  @Test
  public void shouldBeOfTypeSchemaKStreamWhenDataSourceIsKsqlStream() {
    assertThat(realStream.getClass(), equalTo(SchemaKStream.class));
  }

  @Test
  public void shouldBuildStreamWithSameKeyField() {
    // Given:
    final KeyField keyField = KeyField.of("field1", realSchema.field("field1"));

    final StructuredDataSourceNode node = new StructuredDataSourceNode(
        PLAN_NODE_ID,
        SOME_SOURCE,
        realSchema,
        keyField);

    final SchemaKStream<?> stream = node.buildStream(ksqlStreamBuilder);

    // When:
    final KeyField actual = stream.getKeyField();

    // Then:
    assertThat(actual, is(keyField));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    final StructuredDataSourceNode node = new StructuredDataSourceNode(
        PLAN_NODE_ID,
        new KsqlTable<>("sqlExpression", "datasource",
            realSchema,
            KeyField.of("field1", realSchema.field("field1")),
            new LongColumnTimestampExtractionPolicy("timestamp"),
            new KsqlTopic("topic2", "topic2",
                new KsqlJsonTopicSerDe(), false),
            Serdes::String),
        realSchema,
        KeyField.of("field1", realSchema.field("field1")));
    final SchemaKStream result = node.buildStream(ksqlStreamBuilder);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldTransformKStreamToKTableCorrectly() {
    final StructuredDataSourceNode node = new StructuredDataSourceNode(
        PLAN_NODE_ID,
        new KsqlTable<>("sqlExpression", "datasource",
            realSchema,
            KeyField.of("field1", realSchema.field("field1")),
            new LongColumnTimestampExtractionPolicy("timestamp"),
            new KsqlTopic("topic2", "topic2",
                new KsqlJsonTopicSerDe(), false),
            Serdes::String),
        realSchema,
        KeyField.of("field1", realSchema.field("field1")));
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

  @SuppressWarnings("unchecked")
  private StructuredDataSourceNode nodeWithMockTableSource() {
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(streamsBuilder);
    when(streamsBuilder.stream(anyString(), any())).thenReturn((KStream)kStream);

    return new StructuredDataSourceNode(
        realNodeId,
        tableSource,
        realSchema,
        KeyField.of("field1", realSchema.field("field1")),
        materializedFactorySupplier);
  }
}
