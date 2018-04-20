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

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class AggregateNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();

  @Test

  public void shouldBuildSourceNode() {
    build();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topics(), equalTo("[test1]"));
  }

  @Test
  public void shouldHaveOneSubTopologyIfGroupByKey() {
    build();
    final TopologyDescription description = builder.build().describe();
    assertThat(description.subtopologies().size(), equalTo(1));
  }

  @Test
  public void shouldHaveTwoSubTopologies() {
    // We always require rekey at the moment.
    buildRequireRekey();
    final TopologyDescription description = builder.build().describe();
    assertThat(description.subtopologies().size(), equalTo(2));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgy() {
    buildRequireRekey();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000010");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000007")));
    assertThat(node.topics(), containsString("[KSTREAM-AGGREGATE-STATE-STORE-0000000006"));
    assertThat(node.topics(), containsString("-repartition]"));
  }

  @Test
  public void shouldHaveSinkNodeWithSameTopicAsSecondSource() {
    buildRequireRekey();
    TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), "KSTREAM-SINK-0000000008");
    final TopologyDescription.Source source = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000010");
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat("[" + sink.topic() + "]", equalTo(source.topics()));
  }

  @Test
  public void shouldBuildCorrectAggregateSchema() {
    SchemaKStream stream = build();
    final List<Field> expected = Arrays.asList(
        new Field("COL0", 0, Schema.INT64_SCHEMA),
        new Field("KSQL_COL_1", 1, Schema.FLOAT64_SCHEMA),
        new Field("KSQL_COL_2", 2, Schema.INT64_SCHEMA));
    assertThat(stream.getSchema().fields(), equalTo(expected));
  }

  @Test
  public void shouldBeSchemaKTableResult() {
    SchemaKStream stream = build();
    assertThat(stream.getClass(), equalTo(SchemaKTable.class));
  }


  @Test
  public void shouldBeWindowedWhenStatementSpecifiesWindowing() {
    SchemaKStream stream = build();
    assertTrue(((SchemaKTable)stream).isWindowed());
  }

  @Test
  public void shouldFailAggregationOfTable() {
    try {
      buildQuery("SELECT col1, count(col3) FROM test2 GROUP BY col1;");
    } catch (KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo("Unsupported aggregation. KSQL currently only supports aggregation on a Stream."));
    }
  }

  private SchemaKStream build() {
    return buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "WHERE col0 > 100 GROUP BY col0;");
  }

  private SchemaKStream buildRequireRekey() {
    return buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "GROUP BY col1;");
  }

  private SchemaKStream buildQuery(String queryString) {
    AggregateNode aggregateNode = buildAggregateNode(queryString);
    return buildStream(aggregateNode);
  }

  private AggregateNode buildAggregateNode(String queryString) {
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) new LogicalPlanBuilder(MetaStoreFixture.getNewMetaStore()).buildLogicalPlan(queryString);
    return (AggregateNode) planNode.getSource();
  }

  private SchemaKStream buildStream(AggregateNode aggregateNode) {
    return aggregateNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new FunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClient());
  }

}