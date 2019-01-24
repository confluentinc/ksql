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

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KsqlStructuredDataOutputNodeTest {
  private static final String MAPVALUES_OUTPUT_NODE = "KSTREAM-MAPVALUES-0000000003";
  private static final String OUTPUT_NODE = "KSTREAM-SINK-0000000004";
  private static final String QUERY_ID_STRING = "output-test";
  private static final QueryId QUERY_ID = new QueryId(QUERY_ID_STRING);

  private final Schema schema = SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field3", Schema.OPTIONAL_STRING_SCHEMA)
      .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private final KsqlStream dataSource = new KsqlStream<>("sqlExpression", "datasource",
      schema,
      schema.field("key"),
      new LongColumnTimestampExtractionPolicy("timestamp"),
      new KsqlTopic("input", "input",
          new KsqlJsonTopicSerDe(), false), Serdes.String());
  private final StructuredDataSourceNode sourceNode = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      dataSource,
      schema);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private KsqlStructuredDataOutputNode outputNode;

  private SchemaKStream stream;
  private ServiceContext serviceContext;

  @Mock
  private KafkaTopicClient mockTopicClient;
  @Mock
  private QueryIdGenerator queryIdGenerator;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void before() {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short)3);
    serviceContext = TestServiceContext.create(mockTopicClient);
    createOutputNode(props, true);
    when(queryIdGenerator.getNextId()).thenReturn(QUERY_ID_STRING);
    stream = buildStream();
  }

  @After
  public void tearDown() {
    serviceContext.close();
  }

  private void createOutputNode(final Map<String, Object> props, final boolean createInto) {
    outputNode = new KsqlStructuredDataOutputNode(new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        schema.field("key"),
        new KsqlTopic("output", "output", new KsqlJsonTopicSerDe(), true),
        "output",
        props,
        Optional.empty(),
        createInto);
  }

  private SchemaKStream buildStream() {
    reset(mockTopicClient);
    builder = new StreamsBuilder();
    return outputNode.buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        new InternalFunctionRegistry(),
        new HashMap<>(),
        QUERY_ID);
  }

  @Test
  public void shouldBuildSourceNode() {
    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("input")));
  }


  @Test
  public void shouldBuildMapNodePriorToOutput() {
    // Then:
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(builder.build(), MAPVALUES_OUTPUT_NODE),
        Collections.singletonList(TRANSFORM_NODE),
        Collections.singletonList(OUTPUT_NODE));
  }

  @Test
  public void shouldBuildOutputNode() {
    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), OUTPUT_NODE);
    final List<String> predecessors = sink.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(predecessors, equalTo(Collections.singletonList(MAPVALUES_OUTPUT_NODE)));
    assertThat(sink.topic(), equalTo("output"));
  }

  @Test
  public void shouldSetOutputNodeOnStream() {
    // Then:
    assertThat(stream.outputNode(), instanceOf(KsqlStructuredDataOutputNode.class));
  }

  @Test
  public void shouldHaveCorrectOutputNodeSchema() {
    // Then:
    final List<Field> expected = Arrays.asList(
        new Field("ROWTIME", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("ROWKEY", 1, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field1", 2, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field2", 3, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field3", 4, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("timestamp", 5, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("key", 6, Schema.OPTIONAL_STRING_SCHEMA));
    final List<Field> fields = stream.outputNode().getSchema().fields();
    assertThat(fields, equalTo(expected));
  }

  @Test
  public void shouldPartitionByFieldNameInPartitionByProperty() {
    // Given:
    createOutputNode(Collections.singletonMap(DdlConfig.PARTITION_BY_PROPERTY, "field2"), true);

    // When:
    stream = buildStream();

    // Then:
    final Field keyField = stream.getKeyField();
    assertThat(keyField, equalTo(new Field("field2", 1, Schema.OPTIONAL_STRING_SCHEMA)));
    assertThat(stream.getSchema().fields(), equalTo(schema.fields()));
  }

  @Test
  public void shouldCreateSinkTopic() {
    // Then:
    verify(mockTopicClient, times(1)).createTopic(
        eq("output"), eq(4), eq((short)3), eq(Collections.emptyMap()));
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyNonWindowedTable() {
    // Given:
    outputNode = getKsqlStructuredDataOutputNodeForTable(Serdes.String());

    // When:
    stream = buildStream();

    // Then:
    assertThat(stream, instanceOf(SchemaKTable.class));
    final Map<String, String> topicConfig = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    verify(mockTopicClient).createTopic("output", 4, (short) 3, topicConfig);
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyWindowedTable() {
    // Given:
    outputNode = getKsqlStructuredDataOutputNodeForTable(
        WindowedSerdes.timeWindowedSerdeFrom(String.class));

    // When:
    stream = buildStream();

    // Then:
    assertThat(stream, instanceOf(SchemaKTable.class));
    verify(mockTopicClient).createTopic("output", 4, (short) 3, Collections.emptyMap());
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyStream() {
    // Then:
    assertThat(stream, instanceOf(SchemaKStream.class));
    verify(mockTopicClient).createTopic("output", 4, (short) 3, Collections.emptyMap());
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForStream() {
    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("CSAS_0_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForTable() {
    // Given:
    final KsqlStructuredDataOutputNode outputNode
        = getKsqlStructuredDataOutputNodeForTable(Serdes.String());

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("CTAS_0_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForInsertInto() {
    // Given:
    createOutputNode(Collections.emptyMap(), false);

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("InsertQuery_" + QUERY_ID_STRING)));
  }

  private KsqlTopic mockTopic(final KsqlTopicSerDe topicSerde) {
    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getKafkaTopicName()).thenReturn("output");
    when(ksqlTopic.getTopicName()).thenReturn("output");
    when(ksqlTopic.getKsqlTopicSerDe()).thenReturn(topicSerde);
    return ksqlTopic;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldUseCorrectLoggerNameForSerializer() {
    // Given:
    final KsqlTopicSerDe topicSerde = mock(KsqlTopicSerDe.class);
    final Serde serde = mock(Serde.class);
    when(topicSerde.getGenericRowSerde(any(), any(), anyBoolean(), any(), any()))
        .thenReturn(serde);
    outputNode = new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        schema.field("key"),
        mockTopic(topicSerde),
        "output",
        Collections.emptyMap(),
        Optional.empty(),
        false);

    // When:
    buildStream();

    // Then:
    verify(topicSerde)
        .getGenericRowSerde(
            any(),
            any(),
            anyBoolean(),
            any(),
            startsWith(
                QueryLoggerUtil.queryLoggerName(QUERY_ID, outputNode.getId(), "into"))
        );
  }

  private KsqlStructuredDataOutputNode getKsqlStructuredDataOutputNodeForTable(
      final Serde<?> keySerde) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short)3);

    final StructuredDataSourceNode tableSourceNode = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable<>(
            "sqlExpression", "datasource",
            schema,
            schema.field("key"),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic("input", "input", new KsqlJsonTopicSerDe(), false),
            "TableStateStore",
            keySerde),
        schema);

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        tableSourceNode,
        schema,
        new MetadataTimestampExtractionPolicy(),
        schema.field("key"),
        new KsqlTopic("output", "output", new KsqlJsonTopicSerDe(), true),
        "output",
        props,
        Optional.empty(),
        true);
  }
}
