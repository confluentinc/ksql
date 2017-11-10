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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAP_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JoinNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private SchemaKStream stream;
  private JoinNode joinNode;


  @Before
  public void before() {
    String queryString = "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 "
        + "ON t1.col1 = t2.col1;";
    buildJoinNode(queryString);
    stream = buildStream();
  }

  private void buildJoinNode(String queryString) {
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) new LogicalPlanBuilder(MetaStoreFixture.getNewMetaStore()).buildLogicalPlan(queryString);
    joinNode = (JoinNode) ((ProjectNode) planNode.getSource()).getSource();
  }

  @Test
  public void shouldBuildSourceNode() throws Exception {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAP_NODE)));
    assertThat(node.topics(), equalTo("[test2]"));
  }

  @Test
  public void shouldHaveLeftJoin() {
    final Topology topology = builder.build();
    System.out.println(topology.describe());
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "KSTREAM-LEFTJOIN-0000000013");
    final List<String> predecessors = leftJoin.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(leftJoin.stores(), equalTo(Utils.mkSet("KSTREAM-REDUCE-STATE-STORE-0000000003")));
    assertThat(predecessors, equalTo(Collections.singletonList("KSTREAM-SOURCE-0000000012")));
  }

  @Test
  public void shouldHaveAllFieldsFromJoinedInputs() {
    final MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
    final StructuredDataSource source1
        = metaStore.getSource("TEST1");
    final StructuredDataSource source2 = metaStore.getSource("TEST2");
    final Set<String> expected = source1.getSchema()
        .fields().stream()
        .map(field -> "T1."+field.name()).collect(Collectors.toSet());

    expected.addAll(source2.getSchema().fields().stream().map(field -> "T2." + field.name()).collect(Collectors.toSet()));
    final Set<String> fields = stream.getSchema().fields().stream().map(Field::name).collect(Collectors.toSet());
    assertThat(fields, equalTo(expected));
  }

  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();
    return joinNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new MetastoreUtil(),
        new FunctionRegistry(),
        new HashMap<>());
  }

}