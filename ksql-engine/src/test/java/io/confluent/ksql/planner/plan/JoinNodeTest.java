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
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings({"SameParameterValue", "OptionalGetWithoutIsPresent"})
@RunWith(MockitoJUnitRunner.class)
public class JoinNodeTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder;
  private SchemaKStream stream;
  private JoinNode joinNode;

  @Mock
  private KafkaTopicClient mockKafkaTopicClient;

  private static final String leftAlias = "left";
  private static final String rightAlias = "right";

  private final Schema leftSchema = createSchema(leftAlias);
  private final Schema rightSchema = createSchema(rightAlias);
  private final Schema joinSchema = joinSchema();

  private static final KeyField leftKeyField = KeyField
      .of(leftAlias + ".COL0", new Field(leftAlias + ".COL0", 1, Schema.OPTIONAL_STRING_SCHEMA));
  private static final KeyField rightKeyField = KeyField
      .of(rightAlias + ".COL1", new Field(rightAlias + ".COL1", 1, Schema.OPTIONAL_STRING_SCHEMA));

  private static final WithinExpression WITHIN_EXPRESSION =
      new WithinExpression(10, TimeUnit.SECONDS);

  private static final PlanNodeId nodeId = new PlanNodeId("join");
  private static final QueryId queryId = new QueryId("join-query");
  private static final QueryContext.Stacker CONTEXT_STACKER =
      new QueryContext.Stacker(queryId).push(nodeId.toString());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private StructuredDataSourceNode left;
  @Mock
  private StructuredDataSourceNode right;
  @Mock
  private SchemaKStream leftSchemaKStream;
  @Mock
  private SchemaKStream rightSchemaKStream;
  @Mock
  private SchemaKTable leftSchemaKTable;
  @Mock
  private SchemaKTable rightSchemaKTable;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;

  @Before
  public void setUp() {
    builder = new StreamsBuilder();

    final ServiceContext serviceContext = mock(ServiceContext.class);
    when(serviceContext.getTopicClient())
        .thenReturn(mockKafkaTopicClient);

    when(rightSchemaKTable.getKeyField())
        .thenReturn(KeyField.none());

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(ksqlStreamBuilder.getServiceContext()).thenReturn(serviceContext);
    when(ksqlStreamBuilder.withKsqlConfig(any())).thenReturn(ksqlStreamBuilder);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(queryId)
            .push(inv.getArgument(0).toString()));

    when(left.getSchema()).thenReturn(leftSchema);
    when(right.getSchema()).thenReturn(rightSchema);

    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(2);
    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(2);

    setUpSource(left);
    setUpSource(right);

    when(leftSchemaKStream.getKeyField()).thenReturn(leftKeyField);
    when(leftSchemaKTable.getKeyField()).thenReturn(leftKeyField);
    when(rightSchemaKTable.getKeyField()).thenReturn(rightKeyField);
  }

  @Test
  public void shouldThrowIfLeftKeyFieldNotInLeftSchema() {
    // Given:
    final KeyField leftKeyField = KeyField.of(Optional.of("won't find me"), Optional.empty());

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid key field");

    // When:
    new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);
  }

  @Test
  public void shouldThrowIfRightKeyFieldNotInRightSchema() {
    // Given:
    final KeyField rightKeyField = KeyField.of(Optional.of("won't find me"), Optional.empty());

    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid key field");

    // When:
    new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);
  }

  @Test
  public void shouldReturnLeftJoinKeyAsKeyField() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSourceType.KSTREAM,
        DataSourceType.KSTREAM);

    // Then:
    assertThat(joinNode.getKeyField(), is(leftKeyField));
  }

  @Test
  public void shouldBuildSourceNode() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test2")));
  }

  @Test
  public void shouldUseLegacyNameForReduceTopicIfOptimizationsOff() {
    setupTopicClientExpectations(1, 1);
    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(
            ImmutableMap.of(
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
        )
    );

    buildJoin();

    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "KSTREAM-LEFTJOIN-0000000015");
    assertThat(
        leftJoin.stores(),
        equalTo(Utils.mkSet("KSTREAM-AGGREGATE-STATE-STORE-0000000004")));
  }

  @Test
  public void shouldHaveLeftJoin() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "KSTREAM-LEFTJOIN-0000000014");
    final List<String> predecessors = leftJoin.predecessors().stream()
        .map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(leftJoin.stores(), equalTo(Utils.mkSet("KafkaTopic_Right-reduce")));
    assertThat(predecessors, equalTo(Collections.singletonList("KSTREAM-SOURCE-0000000013")));
  }

  @Test
  public void shouldThrowOnPartitionMismatch() {
    setupTopicClientExpectations(1, 2);

    try {
      buildJoin(
          "SELECT t1.col0, t2.col0, t2.col1 "
              + "FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0;"
      );
    } catch (final KsqlException e) {
      Assert.assertThat(e.getMessage(), equalTo(
          "Can't join TEST1 with TEST2 since the number of partitions don't match. TEST1 "
              + "partitions = 1; TEST2 partitions = 2. Please repartition either one so that the "
              + "number of partitions match."
      ));
    }
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
        .map(field -> "T1." + field.name()).collect(Collectors.toSet());

    expected.addAll(source2.getSchema().fields().stream().map(field -> "T2." + field.name())
        .collect(Collectors.toSet()));
    final Set<String> fields = stream.getSchema().fields().stream().map(Field::name)
        .collect(Collectors.toSet());
    assertThat(fields, equalTo(expected));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupStream(right, rightSchemaKStream, rightSchema);

    when(leftSchemaKStream.leftJoin(
        eq(rightSchemaKStream),
        eq(joinSchema),
        eq(leftKeyField),
        eq(WITHIN_EXPRESSION.joinWindow()),
        any(),
        any(),
        eq(CONTEXT_STACKER)))
        .thenReturn(mock(SchemaKStream.class));

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        WITHIN_EXPRESSION,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupStream(right, rightSchemaKStream, rightSchema);

    when(leftSchemaKStream.join(
        eq(rightSchemaKStream),
        eq(joinSchema),
        eq(leftKeyField),
        eq(WITHIN_EXPRESSION.joinWindow()),
        any(),
        any(),
        eq(CONTEXT_STACKER)))
        .thenReturn(mock(SchemaKStream.class));

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.INNER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        WITHIN_EXPRESSION,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupStream(right, rightSchemaKStream, rightSchema);

    when(leftSchemaKStream.outerJoin(
        eq(rightSchemaKStream),
        eq(joinSchema),
        eq(leftKeyField),
        eq(WITHIN_EXPRESSION.joinWindow()),
        any(),
        any(),
        eq(CONTEXT_STACKER)))
        .thenReturn(mock(SchemaKStream.class));

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        WITHIN_EXPRESSION,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @Test
  public void shouldNotPerformStreamStreamJoinWithoutJoinWindow() {
    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.INNER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);

    try {
      joinNode.buildStream(ksqlStreamBuilder);

      fail("Should have raised an exception since no join window was specified");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("Stream-Stream joins must have a WITHIN clause specified"
          + ". None was provided."));
    }

    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @Test
  public void shouldNotPerformJoinIfInputPartitionsMisMatch() {
    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(3);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        WITHIN_EXPRESSION,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KSTREAM);

    try {
      joinNode.buildStream(ksqlStreamBuilder);

      fail("should have raised an exception since the number of partitions on the input sources "
          + "don't match");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("Can't join Foobar with Foobar since the number of "
          + "partitions don't match."));
    }

    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  private static Optional<String> getColumn(final Schema schema, final Predicate<String> filter) {
    return schema.fields().stream()
        .map(Field::name)
        .filter(filter)
        .findFirst();
  }

  private static KeyField getNonKeyColumn(
      final Schema schema,
      final String alias,
      final KeyField keyName
  ) {
    final String prefix = alias + ".";
    final ImmutableList<String> blackList = ImmutableList.of(
        prefix + SchemaUtil.ROWKEY_NAME,
        prefix + SchemaUtil.ROWTIME_NAME,
        keyName.name().get()
    );

    final String column =
        getColumn(schema, s -> !blackList.contains(s))
            .orElseThrow(AssertionError::new);

    final Field field = SchemaUtil.getFieldByName(schema, column).get();
    return KeyField.of(column, field);
  }

  @Test
  public void shouldFailJoinIfTableCriteriaColumnIsNotKey() {
    setupStream(left, leftSchemaKStream, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final KeyField rightCriteriaColumn = getNonKeyColumn(rightSchema, rightAlias, rightKeyField);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightCriteriaColumn,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(ksqlStreamBuilder);

    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
                  rightAlias,
                  rightKeyField.name().get(),
                  rightCriteriaColumn.name().get())));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    when(leftSchemaKStream.leftJoin(
        eq(rightSchemaKTable),
        eq(joinSchema),
        eq(leftKeyField),
        any(),
        eq(CONTEXT_STACKER)))
        .thenReturn(mock(SchemaKStream.class));

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    when(leftSchemaKStream.join(
        eq(rightSchemaKTable),
        eq(joinSchema),
        eq(leftKeyField),
        any(),
        eq(CONTEXT_STACKER)))
        .thenReturn(mock(SchemaKStream.class));

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.INNER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @Test
  public void shouldNotAllowStreamToTableOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    // When:
    try {
      joinNode.buildStream(ksqlStreamBuilder);

      fail("Should have failed to build the stream since stream-table outer joins are not "
          + "supported");
    } catch (final KsqlException e) {
      // Then:
      assertEquals("Full outer joins between streams and tables (stream: left, table: right) are "
          + "not supported.", e.getMessage());
    }

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @Test
  public void shouldNotPerformStreamToTableJoinIfJoinWindowIsSpecified() {
    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(3);


    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(3);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        withinExpression,
        DataSource.DataSourceType.KSTREAM,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(ksqlStreamBuilder);
      fail("should have raised an exception since a join window was provided for a stream-table "
          + "join");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("A window definition was provided for a "
          + "Stream-Table join."));
    }

    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @Test
  public void shouldFailTableTableJoinIfLeftCriteriaColumnIsNotKey() {
    setupTable(left, leftSchemaKTable, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final KeyField leftCriteriaColumn = getNonKeyColumn(leftSchema, leftAlias, leftKeyField);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftCriteriaColumn,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(ksqlStreamBuilder);

    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
                  leftAlias,
                  leftKeyField.name().get(),
                  leftCriteriaColumn.name().get())));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @Test
  public void shouldFailTableTableJoinIfRightCriteriaColumnIsNotKey() {
    setupTable(left, leftSchemaKTable, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final KeyField rightCriteriaColumn = getNonKeyColumn(rightSchema, rightAlias, rightKeyField);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightCriteriaColumn,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(ksqlStreamBuilder);
    } catch (final KsqlException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              String.format(
                  "Source table (%s) key column (%s) is not the column " +
                      "used in the join criteria (%s).",
                  rightAlias,
                  rightKeyField.name().get(),
                  rightCriteriaColumn.name().get())));
      return;
    }
    fail("buildStream did not throw exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    // Given:
    setupTable(left, leftSchemaKTable, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.INNER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);
    when(
        leftSchemaKTable.join(
            eq(rightSchemaKTable),
            eq(joinSchema),
            eq(leftKeyField),
            eq(CONTEXT_STACKER))
    ).thenReturn(mock(SchemaKTable.class));

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.INNER, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    // Given:
    setupTable(left, leftSchemaKTable, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE
    );
    when(leftSchemaKTable.leftJoin(
        eq(rightSchemaKTable),
        eq(joinSchema),
        eq(leftKeyField),
        eq(CONTEXT_STACKER))
    ).thenReturn(mock(SchemaKTable.class));

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.LEFT, joinNode.getJoinType());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    // Given:
    setupTable(left, leftSchemaKTable, leftSchema);
    setupTable(right, rightSchemaKTable, rightSchema);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE
    );
    when(leftSchemaKTable.outerJoin(
        eq(rightSchemaKTable),
        eq(joinSchema),
        eq(leftKeyField),
        eq(CONTEXT_STACKER))
    ).thenReturn(mock(SchemaKTable.class));

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @Test
  public void shouldNotPerformTableToTableJoinIfJoinWindowIsSpecified() {
    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(3);


    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(3);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        withinExpression,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE);

    try {
      joinNode.buildStream(ksqlStreamBuilder);

      fail("should have raised an exception since a join window was provided for a stream-table "
          + "join");
    } catch (final KsqlException e) {
      assertTrue(e.getMessage().startsWith("A window definition was provided for a "
          + "Table-Table join."));
    }

    assertThat(joinNode.getLeftKeyField(), is(leftKeyField));
    assertThat(joinNode.getRightKeyField(), is(rightKeyField));
    assertEquals(leftAlias, joinNode.getLeftAlias());
    assertEquals(rightAlias, joinNode.getRightAlias());
    assertEquals(JoinNode.JoinType.OUTER, joinNode.getJoinType());
  }

  @Test
  public void shouldHaveFullyQualifiedJoinSchema() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        null,
        DataSource.DataSourceType.KTABLE,
        DataSource.DataSourceType.KTABLE
    );

    // When:
    assertThat(joinNode.getSchema(), is(
        SchemaBuilder.struct()
            .field(leftAlias + ".ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(leftAlias + ".ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(leftAlias + ".COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(leftAlias + ".COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field(rightAlias + ".ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(rightAlias + ".ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(rightAlias + ".COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field(rightAlias + ".COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .build()
    ));
  }

  @Test
  public void shouldSelectLeftKeyField() {
    // Given:
    setupStream(left, leftSchemaKStream, leftSchema);
    setupStream(right, rightSchemaKStream, rightSchema);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        JoinNode.JoinType.OUTER,
        left,
        right,
        leftKeyField,
        rightKeyField,
        leftAlias,
        rightAlias,
        WITHIN_EXPRESSION,
        DataSourceType.KSTREAM,
        DataSourceType.KSTREAM
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).selectKey(
        eq(leftKeyField),
        anyBoolean(),
        any()
    );
  }

  @SuppressWarnings("unchecked")
  private void setupTable(
      final StructuredDataSourceNode node,
      final SchemaKTable table,
      final Schema schema
  ) {
    when(node.buildStream(ksqlStreamBuilder)).thenReturn(table);
    when(table.getSchema()).thenReturn(schema);
  }

  @SuppressWarnings("unchecked")
  private void setupStream(
      final StructuredDataSourceNode node,
      final SchemaKStream stream,
      final Schema schema
  ) {
    when(node.buildStream(ksqlStreamBuilder)).thenReturn(stream);
    when(stream.getSchema()).thenReturn(schema);
    when(stream.selectKey(any(), eq(true), any())).thenReturn(stream);
  }

  @SuppressWarnings("Duplicates")
  private Schema joinSchema() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (final Field field : leftSchema.fields()) {
      schemaBuilder.field(field.name(), field.schema());
    }

    for (final Field field : rightSchema.fields()) {
      schemaBuilder.field(field.name(), field.schema());
    }
    return schemaBuilder.build();
  }

  private void buildJoin() {
    buildJoin(
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 "
            + "FROM test1 t1 LEFT JOIN test2 t2 "
            + "ON t1.col1 = t2.col0;"
    );
  }

  private void buildJoin(final String queryString) {
    buildJoinNode(queryString);
    stream = joinNode.buildStream(ksqlStreamBuilder);
  }

  private void buildJoinNode(final String queryString) {
    final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

    final KsqlBareOutputNode planNode =
        (KsqlBareOutputNode) AnalysisTestUtil.buildLogicalPlan(queryString, metaStore);

    joinNode = (JoinNode) ((ProjectNode) planNode.getSource()).getSource();
  }

  private void setupTopicClientExpectations(final int streamPartitions, final int tablePartitions) {
    final Node node = new Node(0, "localhost", 9091);

    final List<TopicPartitionInfo> streamPartitionInfoList =
        IntStream.range(0, streamPartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(),
                    Collections.emptyList()))
            .collect(Collectors.toList());

    when(mockKafkaTopicClient.describeTopic("test1"))
        .thenReturn(new TopicDescription("test1", false, streamPartitionInfoList));

    final List<TopicPartitionInfo> tablePartitionInfoList =
        IntStream.range(0, tablePartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(),
                    Collections.emptyList()))
            .collect(Collectors.toList());

    when(mockKafkaTopicClient.describeTopic("test2"))
        .thenReturn(new TopicDescription("test2", false, tablePartitionInfoList));
  }

  private static void setUpSource(final StructuredDataSourceNode node) {
    final StructuredDataSource structuredDataSource = mock(StructuredDataSource.class);
    when(structuredDataSource.getName()).thenReturn("Foobar");
    when(node.getStructuredDataSource()).thenReturn(structuredDataSource);

    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(structuredDataSource.getKsqlTopic()).thenReturn(ksqlTopic);

    final KsqlTopicSerDe ksqlTopicSerde = mock(KsqlTopicSerDe.class);
    when(ksqlTopic.getKsqlTopicSerDe()).thenReturn(ksqlTopicSerde);
  }

  private static Schema createSchema(final String alias) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field(alias + ".ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field(alias + ".ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field(alias + ".COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field(alias + ".COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA);
    return schemaBuilder.build();
  }
}
