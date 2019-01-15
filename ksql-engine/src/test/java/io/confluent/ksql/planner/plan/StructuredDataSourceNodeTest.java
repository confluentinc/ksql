/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.processing.log.ProcessingLoggerFactory;
import io.confluent.ksql.processing.log.ProcessingLoggerUtil;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StructuredDataSourceNodeTest {
  private static final String TIMESTAMP_FIELD = "timestamp";

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

  private final StructuredDataSourceNode node = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      new KsqlStream<>("sqlExpression", "datasource",
          realSchema,
          realSchema.field("key"),
          new LongColumnTimestampExtractionPolicy("timestamp"),
          new KsqlTopic("topic", "topic",
              new KsqlJsonTopicSerDe(), false), Serdes.String()),
      realSchema);
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
  private KStream kStream;
  @Mock
  private KGroupedStream kGroupedStream;
  @Mock
  private KTable kTable;
  @Mock
  private InternalFunctionRegistry functionRegistry;
  @Mock
  private Function<KsqlConfig, MaterializedFactory> materializedFactorySupplier;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized materialized;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private ServiceContext serviceContext;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    serviceContext = TestServiceContext.create();
    realBuilder = new StreamsBuilder();
    realStream = build(node);

    when(tableSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(tableSource.isWindowed()).thenReturn(false);
    when(tableSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(tableSource.getKeySerde()).thenReturn(keySerde);
    when(tableSource.getTimestampExtractionPolicy()).thenReturn(timestampExtractionPolicy);
    when(ksqlTopic.getKafkaTopicName()).thenReturn("topic");
    when(ksqlTopic.getKsqlTopicSerDe()).thenReturn(topicSerDe);
    when(topicSerDe.getGenericRowSerde(
        any(Schema.class),
        any(KsqlConfig.class),
        any(Boolean.class),
        any(Supplier.class),
        anyString())).thenReturn(rowSerde);
    when(timestampExtractionPolicy.timestampField()).thenReturn(TIMESTAMP_FIELD);
    when(timestampExtractionPolicy.create(anyInt())).thenReturn(timestampExtractor);
    when(streamsBuilder.stream(anyString(), any(Consumed.class))).thenReturn(kStream);
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

  @After
  public void tearDown() {
    serviceContext.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldMaterializeTableCorrectly() {
    // Given:
    final StructuredDataSourceNode node = nodeWithMockTableSource();

    // When:
    node.buildStream(
        streamsBuilder,
        realConfig,
        serviceContext,
        functionRegistry,
        Collections.emptyMap(),
        queryId
    );

    // Then:
    verify(materializedFactorySupplier).apply(realConfig);
    verify(materializedFactory).create(keySerde, rowSerde, "source-reduce");
    verify(kGroupedStream).aggregate(any(), any(), same(materialized));
  }

  @Test
  public void shouldCreateLoggerForSourceSerde() {
    assertThat(
        ProcessingLoggerFactory.getLoggers(),
        hasItem(
            startsWith(
                ProcessingLoggerUtil.join(
                    ProcessingLoggerFactory.PREFIX,
                    QueryLoggerUtil.queryLoggerName(
                        new QueryContext.Builder(queryId)
                            .push(node.getId().toString(), "source")
                            .getQueryContext()
                    )
                )
            )
        )
    );
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
  public void shouldExtracKeyField() {
    assertThat(realStream.getKeyField(), equalTo(new Field("key", 4, Schema.OPTIONAL_STRING_SCHEMA)));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    final StructuredDataSourceNode node = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable<>("sqlExpression", "datasource",
            realSchema,
            realSchema.field("field"),
            new LongColumnTimestampExtractionPolicy("timestamp"),
            new KsqlTopic("topic2", "topic2",
                new KsqlJsonTopicSerDe(), false),
            "statestore",
            Serdes.String()),
        realSchema);
    final SchemaKStream result = build(node);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldTransformKStreamToKTableCorrectly() {
    final StructuredDataSourceNode node = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable<>("sqlExpression", "datasource",
            realSchema,
            realSchema.field("field"),
            new LongColumnTimestampExtractionPolicy("timestamp"),
            new KsqlTopic("topic2", "topic2",
                new KsqlJsonTopicSerDe(), false),
            "statestore",
            Serdes.String()),
        realSchema);
    realBuilder = new StreamsBuilder();
    build(node);
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

  private SchemaKStream build(final StructuredDataSourceNode node) {
    return node.buildStream(
        realBuilder,
        realConfig,
        serviceContext,
        new InternalFunctionRegistry(),
        new HashMap<>(),
        queryId);
  }

  private StructuredDataSourceNode nodeWithMockTableSource() {
    return new StructuredDataSourceNode(
        realNodeId,
        tableSource,
        realSchema,
        materializedFactorySupplier);
  }
}
