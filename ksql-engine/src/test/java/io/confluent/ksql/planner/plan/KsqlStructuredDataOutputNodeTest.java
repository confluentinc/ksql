/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyShort;
import static org.easymock.EasyMock.eq;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class KsqlStructuredDataOutputNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);
  private static final String MAPVALUES_OUTPUT_NODE = "KSTREAM-MAPVALUES-0000000003";
  private static final String OUTPUT_NODE = "KSTREAM-SINK-0000000004";

  private final Schema schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.STRING_SCHEMA)
      .field("field3", Schema.STRING_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .field("key", Schema.STRING_SCHEMA)
      .build();

  private final KsqlStream dataSource = new KsqlStream("sqlExpression", "datasource",
      schema,
      schema.field("key"),
      new LongColumnTimestampExtractionPolicy("timestamp"),
      new KsqlTopic("input", "input",
          new KsqlJsonTopicSerDe()));
  private final StructuredDataSourceNode sourceNode = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      dataSource,
      schema);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private KsqlStructuredDataOutputNode outputNode;

  private SchemaKStream stream;


  @Before
  public void before() {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short)3);
    createOutputNode(props);
    topicClient.createTopic(eq("output"), anyInt(), anyShort(), eq(Collections.emptyMap()));
    EasyMock.expectLastCall();
    EasyMock.replay(topicClient);
    stream = buildStream();
  }

  private void createOutputNode(Map<String, Object> props) {
    outputNode = new KsqlStructuredDataOutputNode(new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        schema.field("key"),
        new KsqlTopic("output", "output", new KsqlJsonTopicSerDe()),
        "output",
        props,
        Optional.empty()
    );
  }

  @Test
  public void shouldBuildSourceNode() {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topics(), equalTo("[input]"));
  }


  @Test
  public void shouldBuildMapNodePriorToOutput() {
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(builder.build(), MAPVALUES_OUTPUT_NODE),
        Collections.singletonList(TRANSFORM_NODE),
        Collections.singletonList(OUTPUT_NODE));
  }

  @Test
  public void shouldBuildOutputNode() {
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), OUTPUT_NODE);
    final List<String> predecessors = sink.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(predecessors, equalTo(Collections.singletonList(MAPVALUES_OUTPUT_NODE)));
    assertThat(sink.topic(), equalTo("output"));
  }

  @Test
  public void shouldSetOutputNodeOnStream() {
    assertThat(stream.outputNode(), instanceOf(KsqlStructuredDataOutputNode.class));
  }

  @Test
  public void shouldHaveCorrectOutputNodeSchema() {
    final List<Field> expected = Arrays.asList(new Field("ROWTIME", 0, Schema.INT64_SCHEMA),
        new Field("ROWKEY", 1, Schema.STRING_SCHEMA),
        new Field("field1", 2, Schema.STRING_SCHEMA),
        new Field("field2", 3, Schema.STRING_SCHEMA),
        new Field("field3", 4, Schema.STRING_SCHEMA),
        new Field("timestamp", 5, Schema.INT64_SCHEMA),
        new Field("key", 6, Schema.STRING_SCHEMA));
    final List<Field> fields = stream.outputNode().getSchema().fields();
    assertThat(fields, equalTo(expected));
  }

  @Test
  public void shouldUpdateReplicationPartitionsInConfig() {
    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo(Integer.valueOf(3).shortValue()));
  }

  @Test
  public void shouldUpdatePartitionsInConfig() {
    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(4));
  }

  @Test
  public void shouldPartitionByFieldNameInPartitionByProperty() {
    createOutputNode(Collections.singletonMap(DdlConfig.PARTITION_BY_PROPERTY, "field2"));
    final SchemaKStream schemaKStream = buildStream();
    final Field keyField = schemaKStream.getKeyField();
    assertThat(keyField, equalTo(new Field("field2", 1, Schema.STRING_SCHEMA)));
    assertThat(schemaKStream.getSchema().fields(), equalTo(schema.fields()));
  }

  @Test
  public void shouldCreateSinkTopic() {
    EasyMock.verify(topicClient);
  }

  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();
    return outputNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new FunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClient());
  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyNonWindowedTable() {
    KafkaTopicClient topicClientForNonWindowTable = EasyMock.mock(KafkaTopicClient.class);
    KsqlStructuredDataOutputNode outputNode = getKsqlStructuredDataOutputNode(false);
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    Map<String, String> topicConfig = ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    topicClientForNonWindowTable.createTopic("output", 4, (short) 3, topicConfig);
    EasyMock.replay(topicClientForNonWindowTable);
    SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        ksqlConfig,
        topicClientForNonWindowTable,
        new FunctionRegistry(),
        new HashMap<>(),
        new MockSchemaRegistryClient());
    assertThat(schemaKStream, instanceOf(SchemaKTable.class));
    EasyMock.verify();

  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyWindowedTable() {
    KafkaTopicClient topicClientForWindowTable = EasyMock.mock(KafkaTopicClient.class);
    KsqlStructuredDataOutputNode outputNode = getKsqlStructuredDataOutputNode(true);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    topicClientForWindowTable.createTopic("output", 4, (short) 3, Collections.emptyMap());
    EasyMock.replay(topicClientForWindowTable);
    SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        ksqlConfig,
        topicClientForWindowTable,
        new FunctionRegistry(),
        new HashMap<>(),
        new MockSchemaRegistryClient());
    assertThat(schemaKStream, instanceOf(SchemaKTable.class));
    EasyMock.verify();

  }

  @Test
  public void shouldCreateSinkWithCorrectCleanupPolicyStream() {
    KafkaTopicClient topicClientForWindowTable = EasyMock.mock(KafkaTopicClient.class);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    topicClientForWindowTable.createTopic("output", 4, (short) 3, Collections.emptyMap());
    EasyMock.replay(topicClientForWindowTable);
    SchemaKStream schemaKStream = outputNode.buildStream(
        streamsBuilder,
        ksqlConfig,
        topicClientForWindowTable,
        new FunctionRegistry(),
        new HashMap<>(),
        new MockSchemaRegistryClient());
    assertThat(schemaKStream, instanceOf(SchemaKStream.class));
    EasyMock.verify();

  }

  private KsqlStructuredDataOutputNode getKsqlStructuredDataOutputNode(boolean isWindowed) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short)3);

    StructuredDataSourceNode tableSourceNode = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable(
            "sqlExpression", "datasource",
            schema,
            schema.field("key"),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic("input", "input", new KsqlJsonTopicSerDe()),
            "TableStateStore",
            isWindowed),
        schema);

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        tableSourceNode,
        schema,
        new MetadataTimestampExtractionPolicy(),
        schema.field("key"),
        new KsqlTopic("output", "output", new KsqlJsonTopicSerDe()),
        "output",
        props,
        Optional.empty());
  }

}