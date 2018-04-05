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

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.easymock.EasyMock.mock;


public class JoinNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private SchemaKStream stream;
  private JoinNode joinNode;

  public void buildJoin() {
    buildJoin("SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 "
        + "ON t1.col1 = t2.col1;");
  }

  public void buildJoin(String queryString) {
    buildJoinNode(queryString);
    stream = buildStream();
  }

  private void buildJoinNode(String queryString) {
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) new LogicalPlanBuilder(MetaStoreFixture.getNewMetaStore()).buildLogicalPlan(queryString);
    joinNode = (JoinNode) ((ProjectNode) planNode.getSource()).getSource();
  }

  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();
    return joinNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new FunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClient());
  }

  private void
  setupTopicClientExpectations(int streamPartitions, int tablePartitions) {
    Node node = new Node(0, "localhost", 9091);

    List<TopicPartitionInfo> streamPartitionInfoList =
        IntStream.range(0, streamPartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(), Collections.emptyList()))
            .collect(Collectors.toList());
    EasyMock.expect(topicClient.describeTopics(Arrays.asList("test1")))
        .andReturn(
            Collections.singletonMap(
                "test1",
                new TopicDescription("test1", false, streamPartitionInfoList)));

    List<TopicPartitionInfo> tablePartitionInfoList =
        IntStream.range(0, tablePartitions)
        .mapToObj(
            p -> new TopicPartitionInfo(p, node, Collections.emptyList(), Collections.emptyList()))
        .collect(Collectors.toList());
    EasyMock.expect(topicClient.describeTopics(Arrays.asList("test2")))
        .andReturn(
            Collections.singletonMap(
                "test2",
                new TopicDescription("test2", false, tablePartitionInfoList)));
    EasyMock.replay(topicClient);
  }

  @Test
  public void shouldBuildSourceNode() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topics(), equalTo("[test2]"));
  }

  @Test
  public void shouldBuildTableNodeWithCorrectAutoCommitOffsetPolicy() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    KsqlConfig ksqlConfig = mock(KsqlConfig.class);
    KafkaTopicClient kafkaTopicClient = mock(KafkaTopicClient.class);
    FunctionRegistry functionRegistry = mock(FunctionRegistry.class);

    class RightTable extends PlanNode {
      final Schema schema;

      public RightTable(final PlanNodeId id, Schema schema) {
        super(id);
        this.schema = schema;
      }
      @Override
      public Schema getSchema() {
        return schema;
      }

      @Override
      public Field getKeyField() {
        return null;
      }

      @Override
      public List<PlanNode> getSources() {
        return null;
      }

      @Override
      public SchemaKStream buildStream(StreamsBuilder builder, KsqlConfig ksqlConfig,
                                       KafkaTopicClient kafkaTopicClient,
                                       FunctionRegistry functionRegistry,
                                       Map<String, Object> props,
                                       SchemaRegistryClient schemaRegistryClient) {
        if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) &&
            props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toString().equalsIgnoreCase
                ("EARLIEST")) {
          return mock(SchemaKTable.class);
        } else {
          throw new KsqlException("auto.offset.reset should be set to EARLIEST.");
        }
      }

      @Override
      protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
        return 1;
      }
    }

    RightTable rightTable = new RightTable(new PlanNodeId("1"), joinNode.getRight().getSchema());

    JoinNode testJoinNode = new JoinNode(joinNode.getId(), joinNode.getType(), joinNode.getLeft()
        , rightTable, joinNode.getLeftKeyFieldName(), joinNode.getRightKeyFieldName(), joinNode
                                             .getLeftAlias(), joinNode.getRightAlias());
    testJoinNode.tableForJoin(builder, ksqlConfig, kafkaTopicClient, functionRegistry,
                          new HashMap<>(), new MockSchemaRegistryClient());

  }

  @Test
  public void shouldHaveLeftJoin() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "KSTREAM-LEFTJOIN-0000000014");
    final List<String> predecessors = leftJoin.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(leftJoin.stores(), equalTo(Utils.mkSet("KSTREAM-REDUCE-STATE-STORE-0000000003")));
    assertThat(predecessors, equalTo(Collections.singletonList("KSTREAM-SOURCE-0000000013")));
  }

  @Test
  public void shouldThrowOnPartitionMismatch() {
    setupTopicClientExpectations(1, 2);

    try {
      buildJoin("SELECT t1.col0, t2.col0, t2.col1 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0;");
    } catch (KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo(
          "Stream and Table have different number of partitions. Either the stream or the table" +
              "must be repartitioned such that both have the same number of partitions."
      ));
    }

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldHaveAllFieldsFromJoinedInputs() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
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
}