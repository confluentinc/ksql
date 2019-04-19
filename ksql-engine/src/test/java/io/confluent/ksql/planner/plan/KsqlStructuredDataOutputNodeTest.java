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

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
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
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlStructuredDataOutputNodeTest {

  private static final String MAPVALUES_OUTPUT_NODE = "KSTREAM-MAPVALUES-0000000003";
  private static final String OUTPUT_NODE = "KSTREAM-SINK-0000000004";
  private static final String QUERY_ID_STRING = "output-test";
  private static final QueryId QUERY_ID = new QueryId(QUERY_ID_STRING);

  private static final String SOURCE_TOPIC_NAME = "input";
  private static final String SOURCE_KAFKA_TOPIC_NAME = "input_kafka";
  private static final String SINK_TOPIC_NAME = "output";
  private static final String SINK_KAFKA_TOPIC_NAME = "output_kafka";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final Schema schema = SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field3", Schema.OPTIONAL_STRING_SCHEMA)
      .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private final KsqlStream dataSource = new KsqlStream<>("sqlExpression", "datasource",
      schema,
      Optional.of(schema.field("key")),
      new LongColumnTimestampExtractionPolicy("timestamp"),
      new KsqlTopic(SOURCE_TOPIC_NAME, SOURCE_KAFKA_TOPIC_NAME,
          new KsqlJsonTopicSerDe(), false), Serdes::String);
  private final StructuredDataSourceNode sourceNode = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      dataSource,
      schema);

  private StreamsBuilder builder;
  private KsqlStructuredDataOutputNode outputNode;

  private SchemaKStream stream;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private QueryIdGenerator queryIdGenerator;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;

  @Before
  public void before() {
    builder = new StreamsBuilder();
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short)3);
    createOutputNode(props, true, new KsqlJsonTopicSerDe());
    when(queryIdGenerator.getNextId()).thenReturn(QUERY_ID_STRING);

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(QUERY_ID)
            .push(inv.getArgument(0).toString()));
  }

  private void createOutputNode(
      final Map<String, Object> props,
      final boolean createInto,
      final KsqlTopicSerDe serde) {
    outputNode = new KsqlStructuredDataOutputNode(new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        Optional.of(schema.field("key")),
        new KsqlTopic(SINK_TOPIC_NAME, SINK_KAFKA_TOPIC_NAME, serde, true),
        SINK_KAFKA_TOPIC_NAME,
        props,
        Optional.empty(),
        createInto);
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of(SOURCE_KAFKA_TOPIC_NAME)));
  }

  @Test
  public void shouldBuildMapNodePriorToOutput() {
    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(builder.build(), MAPVALUES_OUTPUT_NODE),
        Collections.singletonList(TRANSFORM_NODE),
        Collections.singletonList(OUTPUT_NODE));
  }

  @Test
  public void shouldBuildOutputNode() {
    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), OUTPUT_NODE);
    final List<String> predecessors = sink.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(predecessors, equalTo(Collections.singletonList(MAPVALUES_OUTPUT_NODE)));
    assertThat(sink.topic(), equalTo(SINK_KAFKA_TOPIC_NAME));
  }

  @Test
  public void shouldSetOutputNodeOnStream() {
    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(stream.outputNode(), instanceOf(KsqlStructuredDataOutputNode.class));
  }

  @Test
  public void shouldHaveCorrectOutputNodeSchema() {
    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

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
    createOutputNode(
        Collections.singletonMap(DdlConfig.PARTITION_BY_PROPERTY, "field2"),
        true,
        new KsqlJsonTopicSerDe());

    // When:
    stream = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(stream.getKeyField(), is(Optional.of(new Field("field2", 1, Schema.OPTIONAL_STRING_SCHEMA))));
    assertThat(stream.getSchema().fields(), equalTo(schema.fields()));
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
        = getKsqlStructuredDataOutputNodeForTable(Serdes::String);

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("CTAS_0_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForInsertInto() {
    // Given:
    createOutputNode(Collections.emptyMap(), false, new KsqlJsonTopicSerDe());

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("InsertQuery_" + QUERY_ID_STRING)));
  }

  private static KsqlTopic mockTopic(final KsqlTopicSerDe topicSerde) {
    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getKsqlTopicSerDe()).thenReturn(topicSerde);
    return ksqlTopic;
  }

  @Test
  public void shouldBuildOutputNodeForInsertIntoAvroFromNonAvro() {
    // Given:
    //
    // For this case, the properties will be empty (since the analyzer fills the serde
    // properties in based on the source relation.
    createOutputNode(Collections.emptyMap(), false, new KsqlAvroTopicSerDe("name"));

    // When/Then (should not throw):
    outputNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldUseCorrectLoggerNameForSerializer() {
    // Given:
    final KsqlTopicSerDe topicSerde = mock(KsqlTopicSerDe.class);
    outputNode = new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        Optional.ofNullable(schema.field("key")),
        mockTopic(topicSerde),
        "output",
        Collections.emptyMap(),
        Optional.empty(),
        false);

    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(ksqlStreamBuilder).buildGenericRowSerde(
        eq(topicSerde),
        eq(schema),
        queryContextCaptor.capture()
    );

    assertThat(QueryLoggerUtil.queryLoggerName(queryContextCaptor.getValue()), is("output-test.0"));
  }

  private <K> KsqlStructuredDataOutputNode getKsqlStructuredDataOutputNodeForTable(
      final SerdeFactory<K> keySerdeFatory
  ) {
    final Map<String, Object> props = new HashMap<>();
    props.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    props.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);

    final StructuredDataSourceNode tableSourceNode = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable<>(
            "sqlExpression", "datasource",
            schema,
            Optional.ofNullable(schema.field("key")),
            new MetadataTimestampExtractionPolicy(),
            new KsqlTopic(SOURCE_TOPIC_NAME, SOURCE_KAFKA_TOPIC_NAME, new KsqlJsonTopicSerDe(), false),
            keySerdeFatory),
        schema);

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        tableSourceNode,
        schema,
        new MetadataTimestampExtractionPolicy(),
        Optional.ofNullable(schema.field("key")),
        new KsqlTopic(SINK_TOPIC_NAME, SINK_KAFKA_TOPIC_NAME, new KsqlJsonTopicSerDe(), true),
        SINK_KAFKA_TOPIC_NAME,
        props,
        Optional.empty(),
        true);
  }
}