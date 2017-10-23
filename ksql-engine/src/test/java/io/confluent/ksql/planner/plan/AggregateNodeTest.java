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

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAP_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyShort;
import static org.easymock.EasyMock.eq;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertTrue;

public class AggregateNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private SchemaKStream stream;
  private AggregateNode aggregateNode;


  @Before
  public void before() {
    String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "WHERE col0 > 100 GROUP BY col0;";
    buildAggregateNode(queryString);
    stream = buildStream();
  }

  private void buildAggregateNode(String queryString) {
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) new LogicalPlanBuilder(MetaStoreFixture.getNewMetaStore()).buildLogicalPlan(queryString);
    aggregateNode = (AggregateNode) planNode.getSource();
  }

  @Test
  public void shouldBuildSourceNode() throws Exception {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAP_NODE)));
    assertThat(node.topics(), equalTo("[test1]"));
  }

  @Test
  public void shouldHaveTwoSubTopologies() {
    final TopologyDescription description = builder.build().describe();
    assertThat(description.subtopologies().size(), equalTo(2));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgy() {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000006")));
    assertThat(node.topics(), containsString("[KSQL_Agg_Query_"));
    assertThat(node.topics(), containsString("-repartition]"));
  }

  @Test
  public void shouldHaveSinkNodeWithSameTopicAsSecondSource() {
    TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), "KSTREAM-SINK-0000000007");
    final TopologyDescription.Source source = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat("[" + sink.topic() + "]", equalTo(source.topics()));
  }

  @Test
  public void shouldBuildCorrectAggregateSchema() {
    final List<Field> expected = Arrays.asList(new Field("COL0", 0, Schema.INT64_SCHEMA),
        new Field("KSQL_COL_1", 1, Schema.FLOAT64_SCHEMA),
        new Field("KSQL_COL_2", 2, Schema.INT64_SCHEMA));
    assertThat(stream.getSchema().fields(), equalTo(expected));
  }

  @Test
  public void shouldBeSchemaKTableResult() {
    assertThat(stream.getClass(), equalTo(SchemaKTable.class));
  }


  @Test
  public void shouldBeWindowedWhenStatementSpecifiesWindowing() {
    assertTrue(((SchemaKTable)stream).isWindowed());
  }


  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();
    return aggregateNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new MetastoreUtil(),
        new FunctionRegistry(),
        new HashMap<>());
  }

}