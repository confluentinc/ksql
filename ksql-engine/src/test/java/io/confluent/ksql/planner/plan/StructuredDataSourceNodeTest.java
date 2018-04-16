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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;

import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StructuredDataSourceNodeTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private SchemaKStream stream;
  private StreamsBuilder builder;
  private final Schema schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.STRING_SCHEMA)
      .field("field3", Schema.STRING_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .field("key", Schema.STRING_SCHEMA)
      .build();
  private final StructuredDataSourceNode node = new StructuredDataSourceNode(
      new PlanNodeId("0"),
      new KsqlStream("sqlExpression", "datasource",
          schema,
          schema.field("key"),
          new LongColumnTimestampExtractionPolicy("timestamp"),
          new KsqlTopic("topic", "topic",
              new KsqlJsonTopicSerDe())),
      schema);

  @Before
  public void before() {
    builder = new StreamsBuilder();
    stream = build(node);
  }

  private SchemaKStream build(final StructuredDataSourceNode node) {
    return node.buildStream(builder,
        ksqlConfig,
        new FakeKafkaTopicClient(),
        new FunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClient());
  }


  @Test
  public void shouldBuildSourceNode() {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), PlanTestUtil.SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(PlanTestUtil.MAPVALUES_NODE)));
    assertThat(node.topics(), equalTo("[topic]"));
  }

  @Test
  public void shouldBuildMapNode() {
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(builder.build(), PlanTestUtil.MAPVALUES_NODE),
        Collections.singletonList(PlanTestUtil.SOURCE_NODE),
        Collections.singletonList(PlanTestUtil.TRANSFORM_NODE));
  }

  @Test
  public void shouldBuildTransformNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(builder.build(), PlanTestUtil.TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(PlanTestUtil.MAPVALUES_NODE), Collections.emptyList());
  }

  @Test
  public void shouldHaveNoOutputNode() {
    assertThat(stream.outputNode(), nullValue());
  }

  @Test
  public void shouldBeOfTypeSchemaKStreamWhenDataSourceIsKsqlStream() {
    assertThat(stream.getClass(), equalTo(SchemaKStream.class));
  }

  @Test
  public void shouldAddTimestampIndexToConfig() {
    assertThat(ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX), equalTo(1));
  }

  @Test
  public void shouldExtracKeyField() {
    assertThat(stream.getKeyField(), equalTo(new Field("key", 4, Schema.STRING_SCHEMA)));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    StructuredDataSourceNode node = new StructuredDataSourceNode(
        new PlanNodeId("0"),
        new KsqlTable("sqlExpression", "datasource",
            schema,
            schema.field("field"),
            new LongColumnTimestampExtractionPolicy("timestamp"),
            new KsqlTopic("topic2", "topic2",
                new KsqlJsonTopicSerDe()),
            "statestore",
            false),
        schema);
    final SchemaKStream result = build(node);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

}