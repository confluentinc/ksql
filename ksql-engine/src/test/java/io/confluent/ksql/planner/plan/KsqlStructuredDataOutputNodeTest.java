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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
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
import org.junit.Test;

public class KsqlStructuredDataOutputNodeTest {

  private KafkaTopicClient topicClient;

  private static final String MAPVALUES_OUTPUT_NODE = "KSTREAM-MAPVALUES-0000000003";
  private static final String OUTPUT_NODE = "KSTREAM-SINK-0000000004";

  private static final String SOURCE_TOPIC_NAME = "input";
  private static final String SOURCE_KAFKA_TOPIC_NAME = "input_kafka";
  private static final String SINK_TOPIC_NAME = "output";
  private static final String SINK_KAFKA_TOPIC_NAME = "output_kafka";


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
      new KsqlTopic(SOURCE_TOPIC_NAME, SOURCE_KAFKA_TOPIC_NAME,
          new KsqlJsonTopicSerDe(), false), Serdes.String());

  private final StructuredDataSourceNode sourceNode = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      dataSource,
      schema);

  private final KsqlConfig ksqlConfig = new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private KsqlStructuredDataOutputNode outputNode;

  private SchemaKStream stream;
  private ServiceContext serviceContext;

  @Before
  public void before() {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);
    createOutputNode(props);
    topicClient = mock(KafkaTopicClient.class);
    serviceContext = TestServiceContext.create(topicClient);
    final Map<String, TopicDescription> topicDescriptionMap = Collections.singletonMap(SOURCE_KAFKA_TOPIC_NAME, getTopicDescription());
    when(topicClient.describeTopics(any()))
        .thenReturn(topicDescriptionMap);
  }

  @After
  public void tearDown() {
    serviceContext.close();
  }

  private void createOutputNode(final Map<String, Object> props) {
    outputNode = new KsqlStructuredDataOutputNode(new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        schema.field("key"),
        new KsqlTopic(SINK_TOPIC_NAME, SINK_KAFKA_TOPIC_NAME, new KsqlJsonTopicSerDe(), true),
        SINK_KAFKA_TOPIC_NAME,
        props,
        Optional.empty(),
        true);
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    stream = buildStream();

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of(SOURCE_KAFKA_TOPIC_NAME)));
  }


  @Test
  public void shouldBuildMapNodePriorToOutput() {
    // When:
    stream = buildStream();

    // Then:
    verifyProcessorNode(
        (TopologyDescription.Processor) getNodeByName(builder.build(), MAPVALUES_OUTPUT_NODE),
        Collections.singletonList(TRANSFORM_NODE),
        Collections.singletonList(OUTPUT_NODE));
  }

  @Test
  public void shouldBuildOutputNode() {
    // When:
    stream = buildStream();

    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(),
        OUTPUT_NODE);
    final List<String> predecessors = sink.predecessors().stream()
        .map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(predecessors, equalTo(Collections.singletonList(MAPVALUES_OUTPUT_NODE)));
    assertThat(sink.topic(), equalTo(SINK_KAFKA_TOPIC_NAME));
  }

  @Test
  public void shouldSetOutputNodeOnStream() {
    // When:
    stream = buildStream();

    // Then:
    assertThat(stream.outputNode(), instanceOf(KsqlStructuredDataOutputNode.class));
  }

  @Test
  public void shouldHaveCorrectOutputNodeSchema() {
    // Given:
    final List<Field> expected = Arrays.asList(
        new Field("ROWTIME", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("ROWKEY", 1, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field1", 2, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field2", 3, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("field3", 4, Schema.OPTIONAL_STRING_SCHEMA),
        new Field("timestamp", 5, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("key", 6, Schema.OPTIONAL_STRING_SCHEMA));

    // When:
    stream = buildStream();

    // Then:
    final List<Field> fields = stream.outputNode().getSchema().fields();
    assertThat(fields, equalTo(expected));
  }

  @Test
  public void shouldPartitionByFieldNameInPartitionByProperty() {
    // Given:
    createOutputNode(Collections.singletonMap(DdlConfig.PARTITION_BY_PROPERTY, "field2"));

    // When:
    final SchemaKStream schemaKStream = buildStream();

    // Then:
    final Field keyField = schemaKStream.getKeyField();
    assertThat(keyField, equalTo(new Field("field2", 1, Schema.OPTIONAL_STRING_SCHEMA)));
    assertThat(schemaKStream.getSchema().fields(), equalTo(schema.fields()));
  }

  @Test
  public void shouldCreateSinkTopic() {
    // When:
    stream = buildStream();

    // Then:
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 4, (short) 3, Collections.emptyMap());
  }


  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyNonWindowedTable() {
    // Given:
    outputNode = getKsqlStructuredDataOutputNode(Serdes.String());

    // When:
    final SchemaKStream schemaKStream = buildStream();

    // Then:
    final Map<String, String> topicConfig = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 4, (short) 3, topicConfig);
    assertThat(schemaKStream, instanceOf(SchemaKTable.class));
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyWindowedTable() {
    // Given:
    outputNode = getKsqlStructuredDataOutputNode(
        WindowedSerdes.timeWindowedSerdeFrom(String.class));

    // When:
    final SchemaKStream schemaKStream = buildStream();

    // Then:
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 4, (short) 3, Collections.emptyMap());
    assertThat(schemaKStream, instanceOf(SchemaKTable.class));
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyStream() {
    // When:
    final SchemaKStream schemaKStream = buildStream();

    // Then:
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 4, (short) 3, Collections.emptyMap());
    assertThat(schemaKStream, instanceOf(SchemaKStream.class));
  }

  @Test
  public void shouldCreateSinkWithTheSourcePartititionReplication() {
    // Given:
    final TopicDescription topicDescription = getTopicDescription();
    when(topicClient.describeTopics(any()))
        .thenReturn(Collections.singletonMap(SOURCE_KAFKA_TOPIC_NAME, topicDescription));
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    createOutputNode(Collections.emptyMap());

    // When:
    final SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        new KsqlConfig(Collections.emptyMap()),
        TestServiceContext.create(topicClient),
        new InternalFunctionRegistry(),
        new HashMap<>());

    // Then:
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 1, (short) 1, Collections.emptyMap());
    assertThat(schemaKStream, instanceOf(SchemaKStream.class));
  }

  @Test
  public void shouldNotFetchSinkTopicPropsIfProvided() {
    // Given:
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    createOutputNode(ImmutableMap.of(
        KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 5,
        KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3
    ));

    // When:
    final SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        new KsqlConfig(Collections.emptyMap()),
        TestServiceContext.create(topicClient),
        new InternalFunctionRegistry(),
        new HashMap<>());

    // Then:
    verify(topicClient, never()).describeTopics(any());
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 5, (short) 3, Collections.emptyMap());
    assertThat(schemaKStream, instanceOf(SchemaKStream.class));
  }

  @Test
  public void shouldCreateSinkWithTheSourceReplicationAndProvidedPartition() {
    // Given:
    final TopicDescription topicDescription = getTopicDescription();
    when(topicClient.describeTopics(any()))
        .thenReturn(Collections.singletonMap(SOURCE_KAFKA_TOPIC_NAME, topicDescription));
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    createOutputNode(Collections.singletonMap(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 5));

    // When:
    final SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        new KsqlConfig(Collections.emptyMap()),
        TestServiceContext.create(topicClient),
        new InternalFunctionRegistry(),
        new HashMap<>());

    // Then:
    verify(topicClient).createTopic(SINK_KAFKA_TOPIC_NAME, 5, (short) 1, Collections.emptyMap());
    assertThat(schemaKStream, instanceOf(SchemaKStream.class));
  }

  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();

    return outputNode.buildStream(builder,
        ksqlConfig,
        serviceContext,
        new InternalFunctionRegistry(),
        new HashMap<>());
  }

  private KsqlStructuredDataOutputNode getKsqlStructuredDataOutputNode(final Serde<?> keySerde) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);

    final StructuredDataSourceNode tableSourceNode = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable<>(
            "sqlExpression", "datasource",
            schema,
            schema.field("key"),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic(SOURCE_TOPIC_NAME, SOURCE_KAFKA_TOPIC_NAME, new KsqlJsonTopicSerDe(), false),
            "TableStateStore",
            keySerde),
        schema);

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        tableSourceNode,
        schema,
        new MetadataTimestampExtractionPolicy(),
        schema.field("key"),
        new KsqlTopic(SINK_TOPIC_NAME, SINK_KAFKA_TOPIC_NAME, new KsqlJsonTopicSerDe(), true),
        SINK_KAFKA_TOPIC_NAME,
        props,
        Optional.empty(),
        true);
  }

  private static TopicDescription getTopicDescription() {
    final TopicDescription topicDescription = mock(TopicDescription.class);
    final Node node = mock(Node.class);
    final TopicPartitionInfo topicPartitionInfo = mock(TopicPartitionInfo.class);
    when(topicPartitionInfo.replicas()).thenReturn(Collections.singletonList(node));
    when(topicDescription.partitions()).thenReturn(Collections.singletonList(topicPartitionInfo));
    return topicDescription;
  }

}