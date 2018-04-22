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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;

import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class KsqlBareOutputNodeTest {

  private static final String SOURCE_NODE = "KSTREAM-SOURCE-0000000000";
  private static final String SOURCE_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000001";
  private static final String TRANSFORM_NODE = "KSTREAM-TRANSFORMVALUES-0000000002";
  private static final String FILTER_NODE = "KSTREAM-FILTER-0000000003";
  private static final String FILTER_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000004";
  private static final String FOREACH_NODE = "KSTREAM-FOREACH-0000000005";
  private SchemaKStream stream;
  private StreamsBuilder builder;
  private MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private LogicalPlanBuilder planBuilder;

  @Before
  public void before() {
    builder = new StreamsBuilder();
    planBuilder = new LogicalPlanBuilder(metaStore);
    stream = build();
  }

  @Test
  public void shouldBuildSourceNode() throws Exception {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(SOURCE_MAPVALUES_NODE)));
    assertThat(node.topics(), equalTo("[test1]"));
  }

  @Test
  public void shouldBuildMapNode() throws Exception {
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(SOURCE_MAPVALUES_NODE),
        Collections.singletonList(SOURCE_NODE),
        Collections.singletonList(TRANSFORM_NODE));
  }

  @Test
  public void shouldBuildTransformNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(SOURCE_MAPVALUES_NODE), Collections.singletonList(FILTER_NODE));
  }

  @Test
  public void shouldBuildFilterNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(FILTER_NODE);
    verifyProcessorNode(node, Collections.singletonList(TRANSFORM_NODE), Collections.singletonList(FILTER_MAPVALUES_NODE));
  }

  @Test
  public void shouldBuildMapValuesNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(FILTER_MAPVALUES_NODE);
    verifyProcessorNode(node, Collections.singletonList(FILTER_NODE), Collections.singletonList(FOREACH_NODE));
  }

  @Test
  public void shouldBuildForEachNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(FOREACH_NODE);
    verifyProcessorNode(node, Collections.singletonList(FILTER_MAPVALUES_NODE), Collections.emptyList());
  }

  @Test
  public void shouldCreateCorrectSchema() {
    final Schema schema = stream.getSchema();
    assertThat(schema.fields(), equalTo(Arrays.asList(new Field("COL0", 0, Schema.INT64_SCHEMA),
        new Field("COL2", 1, Schema.STRING_SCHEMA),
        new Field("COL3", 2, Schema.FLOAT64_SCHEMA))));
  }

  @Test
  public void shouldSetOutputNode() {
    assertThat(stream.outputNode(), instanceOf(KsqlBareOutputNode.class));
  }

  private SchemaKStream build() {
    final String simpleSelectFilter = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) planBuilder.buildLogicalPlan(simpleSelectFilter);
    return planNode.buildStream(builder, new KsqlConfig(Collections.emptyMap()),
        new FakeKafkaTopicClient(),
        new FunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClient());
  }

  private TopologyDescription.Node getNodeByName(String nodeName) {
    return PlanTestUtil.getNodeByName(builder.build(), nodeName);
  }

}