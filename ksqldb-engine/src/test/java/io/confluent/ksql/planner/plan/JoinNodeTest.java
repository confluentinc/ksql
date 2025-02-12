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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.planner.plan.JoinNode.JoinType.INNER;
import static io.confluent.ksql.planner.plan.JoinNode.JoinType.LEFT;
import static io.confluent.ksql.planner.plan.JoinNode.JoinType.OUTER;
import static io.confluent.ksql.planner.plan.JoinNode.JoinType.RIGHT;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE_FORCE_CHANGELOG;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.plan.JoinNode.JoinKey;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings({"SameParameterValue", "OptionalGetWithoutIsPresent", "unchecked", "rawtypes",
    "checkstyle:ClassDataAbstractionCoupling"})
@RunWith(MockitoJUnitRunner.class)
public class JoinNodeTest {

  protected static final ColumnName SYNTH_KEY = ColumnName.of("ID");

  private static final LogicalSchema LEFT_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("leftKey"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("C0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("L1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("rightKey"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("C0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("R1"), SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema RIGHT2_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("right2Key"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("C0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("R2"), SqlTypes.BIGINT)
      .build();

  private static final SourceName LEFT_ALIAS = SourceName.of("left");
  private static final SourceName RIGHT_ALIAS = SourceName.of("right");
  private static final SourceName RIGHT2_ALIAS = SourceName.of("right2");
  private static final SourceName SINK = SourceName.of("sink");
  private static final KsqlConfig ksqlConfig;

  static {
    final Properties props = new Properties();
    ksqlConfig = new KsqlConfig(props);
  }

  private static final LogicalSchema LEFT_NODE_SCHEMA = prependAlias(
      LEFT_ALIAS, LEFT_SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false)
  );

  private static final LogicalSchema RIGHT_NODE_SCHEMA = prependAlias(
      RIGHT_ALIAS, RIGHT_SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false)
  );

  private static final LogicalSchema RIGHT2_NODE_SCHEMA = prependAlias(
      RIGHT2_ALIAS, RIGHT2_SOURCE_SCHEMA.withPseudoAndKeyColsInValue(false)
  );

  private static final ValueFormat VALUE_FORMAT = ValueFormat
      .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());
  private static final ValueFormat OTHER_FORMAT = ValueFormat
      .of(FormatInfo.of(FormatFactory.DELIMITED.name()), SerdeFeatures.of());

  private StreamsBuilder builder;
  private JoinNode joinNode;

  private static final Optional<WithinExpression> WITHIN_EXPRESSION =
      Optional.of(new WithinExpression(10, TimeUnit.SECONDS));
  private static final WindowTimeClause GRACE_PERIOD = new WindowTimeClause(5, TimeUnit.SECONDS);
  private static final Optional<WithinExpression> WITHIN_EXPRESSION_WITH_GRACE =
      Optional.of(new WithinExpression(10, TimeUnit.SECONDS, GRACE_PERIOD));

  private static final PlanNodeId nodeId = new PlanNodeId("join");
  private static final QueryContext.Stacker CONTEXT_STACKER =
      new QueryContext.Stacker().push(nodeId.toString());

  @Mock
  private DataSource leftSource;
  @Mock
  private DataSource rightSource;
  @Mock
  private DataSourceNode leftSourceNode;
  @Mock
  private DataSourceNode rightSourceNode;
  @Mock
  private DataSourceNode right2SourceNode;
  @Mock
  private PreJoinProjectNode left;
  @Mock
  private PreJoinProjectNode right;
  @Mock
  private PreJoinProjectNode right2;
  @Mock
  private SchemaKStream<String> leftSchemaKStream;
  @Mock
  private SchemaKStream<String> rightSchemaKStream;
  @Mock
  private SchemaKTable<String> leftSchemaKTable;
  @Mock
  private SchemaKTable<String> rightSchemaKTable;
  @Mock
  private PlanBuildContext planBuildContext;
  @Mock
  private RuntimeBuildContext executeContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock
  private ProcessingLogger processLogger;
  @Mock
  private JoinKey joinKey;
  @Mock
  private Projection projection;
  @Mock
  private Expression expression1;
  @Mock
  private Expression expression2;

  @Before
  public void setUp() {
    builder = new StreamsBuilder();

    final ServiceContext serviceContext = mock(ServiceContext.class);
    when(serviceContext.getTopicClient())
        .thenReturn(mockKafkaTopicClient);

    when(planBuildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(planBuildContext.getServiceContext()).thenReturn(serviceContext);
    when(planBuildContext.withKsqlConfig(any())).thenReturn(planBuildContext);
    when(planBuildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(planBuildContext.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));
    when(executeContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(executeContext.getStreamsBuilder()).thenReturn(builder);
    when(executeContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(executeContext.getProcessingLogger(any())).thenReturn(processLogger);

    when(left.getSchema()).thenReturn(LEFT_NODE_SCHEMA);
    when(right.getSchema()).thenReturn(RIGHT_NODE_SCHEMA);
    when(right2.getSchema()).thenReturn(RIGHT2_NODE_SCHEMA);

    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(2);
    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(2);

    when(left.getSourceName()).thenReturn(Optional.of(LEFT_ALIAS));
    when(right.getSourceName()).thenReturn(Optional.of(RIGHT_ALIAS));

    when(joinKey.resolveKeyName(any(), any())).thenReturn(SYNTH_KEY);


    setUpSource(left, VALUE_FORMAT, leftSourceNode, leftSource);
    setUpSource(right, OTHER_FORMAT, rightSourceNode, rightSource);
    setUpSource(right2, OTHER_FORMAT, right2SourceNode, rightSource);
  }

  @Test
  public void shouldBuildSourceNode() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(), SOURCE_NODE_FORCE_CHANGELOG);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KTABLE-SOURCE-0000000002")));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test2")));
  }

  @Test
  public void shouldHaveLeftJoin() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "Join");
    assertThat(leftJoin.stores(), equalTo(ImmutableSet.of("KafkaTopic_Right-Reduce")));
  }

  @Test
  public void shouldThrowOnPartitionMismatch() {
    // Given:
    setupTopicClientExpectations(1, 2);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> buildJoin(
            "SELECT t1.col0, t2.col0, t2.col1 "
                + "FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;"
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Can't join `T1` with `T2` since the number of partitions don't match. "
            + "`T1` partitions = 1; `T2` partitions = 2. Please repartition either one so that the "
            + "number of partitions match."));
  }

  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey, true, left, right, WITHIN_EXPRESSION, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamLeftJoinWithGracePeriod() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey, true, left, right,
            WITHIN_EXPRESSION_WITH_GRACE, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION_WITH_GRACE.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamRightJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, RIGHT, joinKey, true, left, right, WITHIN_EXPRESSION, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).rightJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamRightJoinWithGracePeriod() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, RIGHT, joinKey, true, left, right,
            WITHIN_EXPRESSION_WITH_GRACE, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).rightJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION_WITH_GRACE.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, INNER, joinKey, true, left, right, WITHIN_EXPRESSION, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).innerJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamInnerJoinWithGracePeriod() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, INNER, joinKey, true, left, right,
            WITHIN_EXPRESSION_WITH_GRACE, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).innerJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION_WITH_GRACE.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, OUTER, joinKey, true, left, right, WITHIN_EXPRESSION, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).outerJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToStreamOuterJoinWithGrace() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode =
        new JoinNode(nodeId, OUTER, joinKey, true, left, right,
            WITHIN_EXPRESSION_WITH_GRACE, "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).outerJoin(
        rightSchemaKStream,
        SYNTH_KEY,
        WITHIN_EXPRESSION_WITH_GRACE.get(),
        VALUE_FORMAT.getFormatInfo(),
        OTHER_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldNotPerformStreamStreamJoinWithoutJoinWindow() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);

    final JoinNode joinNode = new JoinNode(nodeId, INNER, joinKey,       true, left,
        right, empty(), "KAFKA");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> joinNode.buildStream(planBuildContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Stream-Stream joins must have a WITHIN clause specified. None was provided."));
  }

  @Test
  public void shouldHandleJoinIfTableHasNoKeyAndJoinFieldIsRowKey() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,        true, left,
        right, empty(), "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        VALUE_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,       true, left,
        right, empty(), "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        VALUE_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, INNER, joinKey,       true, left,
        right, empty(),"KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKStream).innerJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        VALUE_FORMAT.getFormatInfo(),
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldNotAllowStreamToTableOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, OUTER, joinKey,       true, left,
        right, empty(), "KAFKA");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> joinNode.buildStream(planBuildContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid join type encountered: [FULL] OUTER JOIN"));
  }

  @Test
  public void shouldNotPerformStreamToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode =
        new JoinNode(nodeId, OUTER, joinKey,           true, left, right,
            Optional.of(withinExpression),"KAFKA");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> joinNode.buildStream(planBuildContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "A window definition was provided for a Stream-Table join."));
  }

  @Test
  public void shouldPerformTableToTableInnerJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, INNER, joinKey,       true, left,
        right, empty(), "KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKTable).innerJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformTableToTableLeftJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,       true, left,
        right, empty(),"KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKTable).leftJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldPerformTableToTableOuterJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(nodeId, OUTER, joinKey,       true, left,
        right, empty(),"KAFKA");

    // When:
    joinNode.buildStream(planBuildContext);

    // Then:
    verify(leftSchemaKTable).outerJoin(
        rightSchemaKTable,
        SYNTH_KEY,
        CONTEXT_STACKER
    );
  }

  @Test
  public void shouldNotPerformTableToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode =
        new JoinNode(nodeId, OUTER, joinKey,          true, left, right,
            Optional.of(withinExpression),"KAFKA");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> joinNode.buildStream(planBuildContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "A window definition was provided for a Table-Table join."));
  }

  @Test
  public void shouldHaveFullyQualifiedJoinSchemaWithNonSyntheticKey() {
    // Given:
    when(joinKey.resolveKeyName(any(), any())).thenReturn(ColumnName.of("right_rightKey"));

    // When:
    final JoinNode joinNode = new JoinNode(nodeId, OUTER, joinKey,       true, left,
        right, empty(),"KAFKA");

    // When:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("right_rightKey"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_C0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_L1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWPARTITION"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWOFFSET"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_leftKey"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_C0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_R1"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWPARTITION"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWOFFSET"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_rightKey"), SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldHaveFullyQualifiedJoinSchemaWithSyntheticKey() {
    // Given:
    when(joinKey.resolveKeyName(any(), any())).thenReturn(SYNTH_KEY);

    // When:
    final JoinNode joinNode = new JoinNode(nodeId, OUTER, joinKey,       true, left,
        right, empty(),"KAFKA");

    // When:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .keyColumn(SYNTH_KEY, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_C0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_L1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWPARTITION"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_ROWOFFSET"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.text() + "_leftKey"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_C0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_R1"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWPARTITION"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_ROWOFFSET"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.text() + "_rightKey"), SqlTypes.BIGINT)
        .valueColumn(SYNTH_KEY, SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey,          true, left, right,
            WITHIN_EXPRESSION, "KAFKA");

    // Then:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .keyColumn(SYNTH_KEY, SqlTypes.BIGINT)
        .valueColumns(LEFT_NODE_SCHEMA.value())
        .valueColumns(RIGHT_NODE_SCHEMA.value())
        .valueColumn(SYNTH_KEY, SqlTypes.BIGINT)
        .build()));
  }

  @Test
  public void shouldResolveUnaliasedSelectStarByCallingAllSourcesWithValueOnlyFalse() {
    // Given:
    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,       true, left,
        right, empty(),"KAFKA");

    when(left.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("l")));
    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));

    // When:
    final Stream<ColumnName> result = joinNode.resolveSelectStar(empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(ColumnName.of("l"), ColumnName.of("r")));

    verify(left).resolveSelectStar(empty());
    verify(right).resolveSelectStar(empty());
  }

  @Test
  public void shouldResolveUnaliasedSelectStarWithMultipleJoins() {
    // Given:
    final JoinNode inner =
        new JoinNode(new PlanNodeId("foo"), LEFT, joinKey,           true, right,
            right2, empty(), "KAFKA");

    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey,          true, left, inner, empty(),
            "KAFKA");

    when(left.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("l")));
    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));
    when(right2.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r2")));

    // When:
    final Stream<ColumnName> result = joinNode.resolveSelectStar(empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(ColumnName.of("l"), ColumnName.of("r"), ColumnName.of("r2")));

    verify(left).resolveSelectStar(empty());
    verify(right).resolveSelectStar(empty());
    verify(right2).resolveSelectStar(empty());
  }

  @Test
  public void shouldResolveUnaliasedSelectStarWithMultipleJoinsOnLeftSide() {
    // Given:
    final JoinNode inner =
        new JoinNode(new PlanNodeId("foo"), LEFT, joinKey,          true, right,
            right2, empty(), "KAFKA");

    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey,          true, inner, left, empty(),
            "KAFKA");

    when(left.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("l")));
    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));
    when(right2.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r2")));

    // When:
    final Stream<ColumnName> result = joinNode.resolveSelectStar(empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(ColumnName.of("r"), ColumnName.of("r2"), ColumnName.of("l")));

    verify(left).resolveSelectStar(empty());
    verify(right).resolveSelectStar(empty());
    verify(right2).resolveSelectStar(empty());
  }

  @Test
  public void shouldResolveAliasedSelectStarByCallingOnlyCorrectParent() {
    // Given:
    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,      true, left, right,
        empty(),"KAFKA");

    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));

    // When:
    final Stream<ColumnName> result = joinNode.resolveSelectStar(Optional.of(RIGHT_ALIAS));

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(ColumnName.of("r")));

    verify(left, never()).resolveSelectStar(any());
    verify(right).resolveSelectStar(Optional.of(RIGHT_ALIAS));
  }

  @Test
  public void shouldIncludeSyntheticJoinKeyInResolvedSelectStart() {
    // Given:
    when(joinKey.isSynthetic()).thenReturn(true);

    final JoinNode joinNode = new JoinNode(nodeId, OUTER, joinKey,       true, left,
        right, empty(),"KAFKA");

    when(left.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("l")));
    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));

    // When:
    final Stream<ColumnName> result = joinNode.resolveSelectStar(empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(SYNTH_KEY, ColumnName.of("l"), ColumnName.of("r")));
  }

  @Test
  public void shouldResolveNestedAliasedSelectStarByCallingOnlyCorrectParentWithMultiJoins() {
    // Given:
    final JoinNode inner =
        new JoinNode(new PlanNodeId("foo"), LEFT, joinKey,          true, right,
            right2, empty(), "KAFKA");

    final JoinNode joinNode =
        new JoinNode(nodeId, LEFT, joinKey,         true, left, inner, empty(),
            "KAFKA");

    when(right.resolveSelectStar(any())).thenReturn(Stream.of(ColumnName.of("r")));

    // When:
    final Stream<ColumnName> result = joinNode
        .resolveSelectStar(Optional.of(RIGHT_ALIAS));

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(ColumnName.of("r")));

    verify(left, never()).resolveSelectStar(any());
    verify(right2, never()).resolveSelectStar(any());
    verify(right).resolveSelectStar(Optional.of(RIGHT_ALIAS));
  }

  @Test
  public void shouldTestExpressionForBothOriginalAndRewrittenJoinKeys() {
    // Given:
    when(projection.containsExpression(any())).thenReturn(false, true);

    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey,      true, left,
        right, empty(),"KAFKA");

    when(joinKey.getAllViableKeys(any()))
        .thenReturn((List) ImmutableList.of(expression1, expression2));

    // When:
    joinNode.validateKeyPresent(SINK, projection);

    // Then:
    verify(projection).containsExpression(expression2);
    verify(projection).containsExpression(expression1);
  }

  @Test
  public void shouldThrowIfProjectionDoesNotIncludeAnyJoinColumns() {
    // Given:
    final JoinNode joinNode = new JoinNode(nodeId, LEFT, joinKey, true, left, right,
        empty(),"KAFKA");

    when(joinKey.getAllViableKeys(any()))
        .thenReturn((List) ImmutableList.of(expression1, expression2));
    when(projection.containsExpression(any())).thenReturn(false);
    when(joinKey.getOriginalViableKeys(any()))
        .thenReturn((List) ImmutableList.of(expression1, expression1, expression2));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> joinNode.validateKeyPresent(SINK, projection)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The query used to build `sink` "
        + "must include the join expressions expression1, expression1 or expression2 "
        + "in its projection (eg, SELECT expression1..."));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void setupTable(
      final PlanNode node,
      final SchemaKTable<?> table
  ) {
    when(node.buildStream(planBuildContext)).thenReturn((SchemaKTable) table);
    when(node.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
  }

  @SuppressWarnings("unchecked")
  private void setupStream(
      final PlanNode node,
      final SchemaKStream stream
  ) {
    when(node.buildStream(planBuildContext)).thenReturn(stream);
    when(node.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
  }

  private void buildJoin() {
    buildJoin(
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 "
            + "FROM test1 t1 LEFT JOIN test2 t2 "
            + "ON t1.col0 = t2.col0 EMIT CHANGES;"
    );
  }

  private void buildJoin(final String queryString) {
    buildJoinNode(queryString);
    final SchemaKStream<?> stream = joinNode.buildStream(planBuildContext);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable<?> table = (SchemaKTable<?>) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(executeContext));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(executeContext));
    }
  }

  private void buildJoinNode(final String queryString) {
    final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

    final KsqlBareOutputNode planNode =
        (KsqlBareOutputNode) AnalysisTestUtil.buildLogicalPlan(ksqlConfig, queryString, metaStore);

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

  private static void setUpSource(
      final PlanNode node,
      final ValueFormat valueFormat,
      final DataSourceNode dataSourceNode,
      final DataSource dataSource
  ) {
    when(dataSourceNode.getDataSource()).thenReturn(dataSource);
    when(node.getLeftmostSourceNode()).thenReturn(dataSourceNode);
    when(node.getSources()).thenReturn(ImmutableList.of(dataSourceNode));

    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getValueFormat()).thenReturn(valueFormat);

    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
  }

  private static LogicalSchema prependAlias(final SourceName alias, final LogicalSchema schema) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();
    builder.keyColumns(schema.key());
    for (final Column c : schema.value()) {
      builder.valueColumn(ColumnNames.generatedJoinColumnAlias(alias, c.name()), c.type());
    }
    return builder.build();
  }
}
