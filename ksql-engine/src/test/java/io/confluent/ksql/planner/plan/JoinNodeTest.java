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

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class JoinNodeTest {
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);

  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder = new StreamsBuilder();
  private SchemaKStream stream;
  private JoinNode joinNode;

  private StreamsBuilder mockStreamsBuilder;
  private KsqlConfig mockKsqlConfig;
  private KafkaTopicClient mockKafkaTopicClient;
  private FunctionRegistry mockFunctionRegistry;
  private Supplier<SchemaRegistryClient> mockSchemaRegistryClientFactory;
  private final Schema leftSchema = createSchema();
  private final Schema rightSchema = createSchema();
  private final Schema joinSchema = joinSchema();

  private final String leftAlias = "left";
  private final String rightAlias = "right";

  private final String leftKeyFieldName = "COL0";
  private final String rightKeyFieldName = "COL1";

  private Map<String, Object> properties;
  private StructuredDataSourceNode left;
  private StructuredDataSourceNode right;
  private SchemaKStream leftSchemaKStream;
  private SchemaKStream rightSchemaKStream;
  private SchemaKTable leftSchemaKTable;
  private SchemaKTable rightSchemaKTable;
  private Field joinKey;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    mockStreamsBuilder = niceMock(StreamsBuilder.class);
    mockKsqlConfig = niceMock(KsqlConfig.class);
    mockKafkaTopicClient = niceMock(KafkaTopicClient.class);
    mockFunctionRegistry = niceMock(FunctionRegistry.class);
    mockSchemaRegistryClientFactory = niceMock(Supplier.class);

    properties = new HashMap<>();

    left = niceMock(StructuredDataSourceNode.class);
    right = niceMock(StructuredDataSourceNode.class);
    leftSchemaKStream = niceMock(SchemaKStream.class);
    rightSchemaKStream = niceMock(SchemaKStream.class);
    leftSchemaKTable = niceMock(SchemaKTable.class);
    rightSchemaKTable = niceMock(SchemaKTable.class);

    joinKey = joinSchema.field(leftAlias + "." + leftKeyFieldName);

  }

  public void buildJoin() {
    buildJoin("SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 "
        + "ON t1.col1 = t2.col0;");
  }

  public void buildJoin(final String queryString) {
    buildJoinNode(queryString);
    stream = buildStream();
  }

  private void buildJoinNode(final String queryString) {
    final KsqlBareOutputNode planNode =
        (KsqlBareOutputNode) new LogicalPlanBuilder(
            MetaStoreFixture.getNewMetaStore(
                new InternalFunctionRegistry())).buildLogicalPlan(queryString);
    joinNode = (JoinNode) ((ProjectNode) planNode.getSource()).getSource();
  }

  private SchemaKStream buildStream() {
    builder = new StreamsBuilder();
    return joinNode.buildStream(builder,
        ksqlConfig,
        topicClient,
        new InternalFunctionRegistry(),
        new HashMap<>(), new MockSchemaRegistryClientFactory()::get);
  }

  private void
  setupTopicClientExpectations(final int streamPartitions, final int tablePartitions) {
    final Node node = new Node(0, "localhost", 9091);

    final List<TopicPartitionInfo> streamPartitionInfoList =
        IntStream.range(0, streamPartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(), Collections.emptyList()))
            .collect(Collectors.toList());
    expect(topicClient.describeTopics(Arrays.asList("test1")))
        .andReturn(
            Collections.singletonMap(
                "test1",
                new TopicDescription("test1", false, streamPartitionInfoList)));

    final List<TopicPartitionInfo> tablePartitionInfoList =
        IntStream.range(0, tablePartitions)
        .mapToObj(
            p -> new TopicPartitionInfo(p, node, Collections.emptyList(), Collections.emptyList()))
        .collect(Collectors.toList());
    expect(topicClient.describeTopics(Arrays.asList("test2")))
        .andReturn(
            Collections.singletonMap(
                "test2",
                new TopicDescription("test2", false, tablePartitionInfoList)));
    replay(topicClient);
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
  public void shouldHaveLeftJoin() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "KSTREAM-LEFTJOIN-0000000015");
    final List<String> predecessors = leftJoin.predecessors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(leftJoin.stores(), equalTo(Utils.mkSet("KSTREAM-AGGREGATE-STATE-STORE-0000000004")));
    assertThat(predecessors, equalTo(Collections.singletonList("KSTREAM-SOURCE-0000000014")));
  }

  @Test
  public void shouldThrowOnPartitionMismatch() {
    setupTopicClientExpectations(1, 2);

    try {
      buildJoin("SELECT t1.col0, t2.col0, t2.col1 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0;");
    } catch (final KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo(
          "Can't join TEST1 with TEST2 since the number of partitions don't match. TEST1 "
          + "partitions = 1; TEST2 partitions = 2. Please repartition either one so that the "
          + "number of partitions match."
      ));
    }

    verify(topicClient);
  }

  @Test
  public void shouldHaveAllFieldsFromJoinedInputs() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
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

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    expectKeyField(leftSchemaKStream, leftKeyFieldName);

    setupStream(right, rightSchemaKStream, rightSchema, 2);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    expect(leftSchemaKStream.leftJoin(eq(rightSchemaKStream),
                                      eq(joinSchema),
                                      eq(joinKey),
                                      eq(withinExpression.joinWindow()),
                                      anyObject(Serde.class),
                                      anyObject(Serde.class)))
        .andReturn(niceMock(SchemaKStream.class));

    replay(left, right, leftSchemaKStream, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.LEFT,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KSTREAM);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKStream, rightSchemaKStream);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    expectKeyField(leftSchemaKStream, leftKeyFieldName);

    setupStream(right, rightSchemaKStream, rightSchema, 2);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    expect(leftSchemaKStream.join(eq(rightSchemaKStream),
                                  eq(joinSchema),
                                  eq(joinKey),
                                  eq(withinExpression.joinWindow()),
                                  anyObject(Serde.class),
                                  anyObject(Serde.class)))
        .andReturn(niceMock(SchemaKStream.class));

    replay(left, right, leftSchemaKStream, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.INNER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KSTREAM);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKStream, rightSchemaKStream);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    expectKeyField(leftSchemaKStream, leftKeyFieldName);

    setupStream(right, rightSchemaKStream, rightSchema, 2);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    expect(leftSchemaKStream.outerJoin(eq(rightSchemaKStream),
                                       eq(joinSchema),
                                       eq(joinKey),
                                       eq(withinExpression.joinWindow()),
                                       anyObject(Serde.class),
                                       anyObject(Serde.class)))
        .andReturn(niceMock(SchemaKStream.class));

    replay(left, right, leftSchemaKStream, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KSTREAM);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKStream, rightSchemaKStream);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotPerformStreamStreamJoinWithoutJoinWindow() {
    expect(left.getSchema()).andReturn(leftSchema);
    expect(left.getPartitions(mockKafkaTopicClient)).andReturn(2);

    expect(right.getSchema()).andReturn(rightSchema);
    expect(right.getPartitions(mockKafkaTopicClient)).andReturn(2);

    replay(left, right, leftSchemaKStream, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.INNER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KSTREAM);

    try {
      joinNode.buildStream(mockStreamsBuilder,
                           mockKsqlConfig,
                           mockKafkaTopicClient,
                           mockFunctionRegistry,
                           properties,
                           mockSchemaRegistryClientFactory);
      fail("Should have raised an exception since no join window was specified");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("Stream-Stream joins must have a WITHIN clause specified"
                                           + ". None was provided."));
    }

    verify(left, right, leftSchemaKStream, rightSchemaKStream);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }


  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotPerformJoinIfInputPartitionsMisMatch() {
    expect(left.getSchema()).andReturn(leftSchema);
    expect(left.getPartitions(mockKafkaTopicClient)).andReturn(3);

    expect(right.getSchema()).andReturn(rightSchema);
    expect(right.getPartitions(mockKafkaTopicClient)).andReturn(2);

    expectSourceName(left);
    expectSourceName(right);
    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    replay(left, right, leftSchemaKStream, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KSTREAM);

    try {
      joinNode.buildStream(mockStreamsBuilder,
                           mockKsqlConfig,
                           mockKafkaTopicClient,
                           mockFunctionRegistry,
                           properties,
                           mockSchemaRegistryClientFactory);
      fail("should have raised an exception since the number of partitions on the input sources "
           + "don't match");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("Can't join Foobar with Foobar since the number of "
                                           + "partitions don't match."));
    }

    verify(left, right, leftSchemaKStream, rightSchemaKStream);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  private static Optional<String> getColumn(final Schema schema, final Predicate<String> filter) {
    return schema.fields().stream()
        .map(Field::name)
        .filter(filter::test)
        .findFirst();
  }

  private static Optional<String> getNonKeyColumn(final Schema schema, final String keyName) {
    return getColumn(
        schema,
        s -> !ImmutableList.of(SchemaUtil.ROWKEY_NAME, SchemaUtil.ROWTIME_NAME, keyName).contains(s)
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldFailJoinIfTableCriteriaColumnIsNotKey() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    setupTable(right, rightSchemaKTable, rightSchema, 2);
    expectKeyField(rightSchemaKTable, rightKeyFieldName);
    replay(left, right, leftSchemaKStream, rightSchemaKTable);

    final String rightCriteriaColumn = getNonKeyColumn(rightSchema, rightKeyFieldName).get();
    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyFieldName,
        rightCriteriaColumn,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
          mockKsqlConfig,
          mockKafkaTopicClient,
          mockFunctionRegistry,
          properties,
          mockSchemaRegistryClientFactory);
    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
              rightAlias,
              rightKeyFieldName,
              rightCriteriaColumn)));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    expectKeyField(leftSchemaKStream, leftKeyFieldName);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    expect(leftSchemaKStream.leftJoin(eq(rightSchemaKTable),
                                      eq(joinSchema),
                                      eq(joinKey),
                                      anyObject(Serde.class)))
        .andReturn(niceMock(SchemaKStream.class));

    replay(left, right, leftSchemaKStream, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.LEFT,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KTABLE);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKStream, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    setupStream(left, leftSchemaKStream, leftSchema, 2);
    expectKeyField(leftSchemaKStream, leftKeyFieldName);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    expect(leftSchemaKStream.join(eq(rightSchemaKTable),
                                  eq(joinSchema),
                                  eq(joinKey),
                                  anyObject(Serde.class)))
        .andReturn(niceMock(SchemaKStream.class));

    replay(left, right, leftSchemaKStream, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.INNER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KTABLE);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKStream, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotAllowStreamToTableOuterJoin() {
    setupStreamWithoutSerde(left, leftSchemaKStream, leftSchema, 2);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    replay(left, right, leftSchemaKStream, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
                           mockKsqlConfig,
                           mockKafkaTopicClient,
                           mockFunctionRegistry,
                           properties,
                           mockSchemaRegistryClientFactory);
      fail("Should have failed to build the stream since stream-table outer joins are not "
           + "supported");
    } catch (final KsqlException e) {
      assertEquals("Full outer joins between streams and tables (stream: left, table: right) are "
                   + "not supported.", e.getMessage());
    }

    verify(left, right, leftSchemaKStream, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotPerformStreamToTableJoinIfJoinWindowIsSpecified() {
    expect(left.getSchema()).andReturn(leftSchema);
    expect(left.getPartitions(mockKafkaTopicClient)).andReturn(3);

    expect(right.getSchema()).andReturn(rightSchema);
    expect(right.getPartitions(mockKafkaTopicClient)).andReturn(3);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    replay(left, right, leftSchemaKStream, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KSTREAM,
                                           DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
                           mockKsqlConfig,
                           mockKafkaTopicClient,
                           mockFunctionRegistry,
                           properties,
                           mockSchemaRegistryClientFactory);
      fail("should have raised an exception since a join window was provided for a stream-table "
           + "join");
    } catch (final KsqlException e) {
        assertTrue(e.getMessage().startsWith("A window definition was provided for a "
                                             + "Stream-Table join."));
    }

    verify(left, right, leftSchemaKStream, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldFailTableTableJoinIfLeftCriteriaColumnIsNotKey() {
    setupTable(left, leftSchemaKTable, leftSchema, 2);
    expectKeyField(leftSchemaKTable, leftKeyFieldName);
    setupTable(right, rightSchemaKTable, rightSchema, 2);
    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final String leftCriteriaColumn = getNonKeyColumn(leftSchema, leftKeyFieldName).get();
    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftCriteriaColumn,
        rightKeyFieldName,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
          mockKsqlConfig,
          mockKafkaTopicClient,
          mockFunctionRegistry,
          properties,
          mockSchemaRegistryClientFactory);
    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
                  leftAlias,
                  leftKeyFieldName,
                  leftCriteriaColumn)));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldFailTableTableJoinIfRightCriteriaColumnIsNotKey() {
    setupTable(left, leftSchemaKTable, leftSchema, 2);
    expectKeyField(leftSchemaKTable, leftKeyFieldName);
    setupTable(right, rightSchemaKTable, rightSchema, 2);
    expectKeyField(rightSchemaKTable, rightKeyFieldName);
    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final String rightCriteriaColumn = getNonKeyColumn(rightSchema, rightKeyFieldName).get();
    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyFieldName,
        rightCriteriaColumn,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
          mockKsqlConfig,
          mockKafkaTopicClient,
          mockFunctionRegistry,
          properties,
          mockSchemaRegistryClientFactory);
    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
                  rightAlias,
                  rightKeyFieldName,
                  rightCriteriaColumn)));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    setupTable(left, leftSchemaKTable, leftSchema, 2);
    expectKeyField(leftSchemaKTable, leftKeyFieldName);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    expect(leftSchemaKTable.join(eq(rightSchemaKTable),
                                 eq(joinSchema),
                                 eq(joinKey)))
        .andReturn(niceMock(SchemaKTable.class));

    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.INNER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KTABLE,
                                           DataSource.DataSourceType.KTABLE);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKTable, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    setupTable(left, leftSchemaKTable, leftSchema, 2);
    expectKeyField(leftSchemaKTable, leftKeyFieldName);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    expect(leftSchemaKTable.leftJoin(eq(rightSchemaKTable),
                                     eq(joinSchema),
                                     eq(joinKey)))
        .andReturn(niceMock(SchemaKTable.class));

    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.LEFT,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KTABLE,
                                           DataSource.DataSourceType.KTABLE);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKTable, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    setupTable(left, leftSchemaKTable, leftSchema, 2);
    expectKeyField(leftSchemaKTable, leftKeyFieldName);

    setupTable(right, rightSchemaKTable, rightSchema, 2);

    expect(leftSchemaKTable.outerJoin(eq(rightSchemaKTable),
                                      eq(joinSchema),
                                      eq(joinKey)))
        .andReturn(niceMock(SchemaKTable.class));

    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           null,
                                           DataSource.DataSourceType.KTABLE,
                                           DataSource.DataSourceType.KTABLE);

    joinNode.buildStream(mockStreamsBuilder,
                         mockKsqlConfig,
                         mockKafkaTopicClient,
                         mockFunctionRegistry,
                         properties,
                         mockSchemaRegistryClientFactory);

    verify(left, right, leftSchemaKTable, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotPerformTableToTableJoinIfJoinWindowIsSpecified() {
    expect(left.getSchema()).andReturn(leftSchema);
    expect(left.getPartitions(mockKafkaTopicClient)).andReturn(3);

    expect(right.getSchema()).andReturn(rightSchema);
    expect(right.getPartitions(mockKafkaTopicClient)).andReturn(3);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    replay(left, right, leftSchemaKTable, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(new PlanNodeId("join"),
                                           JoinNode.JoinType.OUTER,
                                           left,
                                           right,
                                           leftKeyFieldName,
                                           rightKeyFieldName,
                                           leftAlias,
                                           rightAlias,
                                           withinExpression,
                                           DataSource.DataSourceType.KTABLE,
                                           DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(mockStreamsBuilder,
                           mockKsqlConfig,
                           mockKafkaTopicClient,
                           mockFunctionRegistry,
                           properties,
                           mockSchemaRegistryClientFactory);
      fail("should have raised an exception since a join window was provided for a stream-table "
           + "join");
    } catch (final KsqlException e) {
        assertTrue(e.getMessage().startsWith("A window definition was provided for a "
                                             + "Table-Table join."));
    }

    verify(left, right, leftSchemaKTable, rightSchemaKTable);

    assertEquals(leftKeyFieldName, joinNode.getLeftKeyFieldName());
    assertEquals(rightKeyFieldName, joinNode.getRightKeyFieldName());
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }


  private void setupTable(final StructuredDataSourceNode node, final SchemaKTable table,
                          final Schema schema, final int partitions) {
    expect(node.getSchema()).andReturn(schema);
    expect(node.getPartitions(mockKafkaTopicClient)).andReturn(partitions);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    expect(node.buildStream(mockStreamsBuilder,
                            mockKsqlConfig,
                            mockKafkaTopicClient,
                            mockFunctionRegistry,
                            properties,
                            mockSchemaRegistryClientFactory))
        .andReturn(table);
  }

  private void expectSourceName(final StructuredDataSourceNode node) {
    final StructuredDataSource dataSource = niceMock(StructuredDataSource.class);
    expect(node.getStructuredDataSource()).andReturn(dataSource).anyTimes();

    expect(dataSource.getName()).andReturn("Foobar").anyTimes();
    replay(dataSource);
  }

  private void setupStream(final StructuredDataSourceNode node,
                           final SchemaKStream stream, final Schema schema, final int partitions) {
    setupStreamWithoutSerde(node, stream, schema, partitions);
    expectGetSerde(node, schema);
  }

  private void setupStreamWithoutSerde(final StructuredDataSourceNode node,
                                       final SchemaKStream stream, final Schema schema,
                                       final int partitions) {
    expect(node.getSchema()).andReturn(schema);
    expect(node.getPartitions(mockKafkaTopicClient)).andReturn(partitions);
    expectBuildStream(node, stream, schema, properties);
  }


  private void expectKeyField(final SchemaKStream stream, final String keyFieldName) {
    final Field field = niceMock(Field.class);
    expect(stream.getKeyField()).andStubReturn(field);
    expect(field.name()).andStubReturn(keyFieldName);
    replay(field);
  }

  private Schema joinSchema() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (final Field field : leftSchema.fields()) {
      final String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (final Field field : rightSchema.fields()) {
      final String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private void expectGetSerde(final StructuredDataSourceNode node,
                              final Schema schema) {
    final StructuredDataSource structuredDataSource = niceMock(StructuredDataSource.class);
    expect(node.getStructuredDataSource()).andReturn(structuredDataSource);

    final KsqlTopic ksqlTopic = niceMock(KsqlTopic.class);
    expect(structuredDataSource.getKsqlTopic()).andReturn(ksqlTopic);

    final KsqlTopicSerDe ksqlTopicSerde = niceMock(KsqlTopicSerDe.class);
    expect(ksqlTopic.getKsqlTopicSerDe()).andReturn(ksqlTopicSerde);

    final Serde<GenericRow> serde = niceMock(Serde.class);
    expect(node.getSchema()).andReturn(schema);
    expect(ksqlTopicSerde.getGenericRowSerde(schema, ksqlConfig, false, mockSchemaRegistryClientFactory))
        .andReturn(serde);
    replay(structuredDataSource, ksqlTopic, ksqlTopicSerde);
  }

  private void expectBuildStream(final StructuredDataSourceNode node, final SchemaKStream result,
                                 final Schema schema, final Map<String, Object> properties) {
    expect(node.buildStream(mockStreamsBuilder,
                            mockKsqlConfig,
                            mockKafkaTopicClient,
                            mockFunctionRegistry,
                            properties,
                            mockSchemaRegistryClientFactory))
        .andReturn(result);

    expect(result.getSchema()).andReturn(schema);
    expect(result.selectKey(anyObject(Field.class), eq(true)))
        .andReturn(result);
  }

  private Schema createSchema() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA);
    return schemaBuilder.build();
  }
}
