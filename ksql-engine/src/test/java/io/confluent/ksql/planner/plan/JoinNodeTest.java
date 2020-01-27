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
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
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

  private static final LogicalSchema LEFT_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("C0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("L1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("C0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("R1"), SqlTypes.BIGINT)
      .build();

  private static final SourceName LEFT_ALIAS = SourceName.of("left");
  private static final SourceName RIGHT_ALIAS = SourceName.of("right");

  private static final LogicalSchema LEFT_NODE_SCHEMA = prependAlias(
      LEFT_ALIAS, LEFT_SOURCE_SCHEMA.withMetaAndKeyColsInValue(false)
  );

  private static final LogicalSchema RIGHT_NODE_SCHEMA = prependAlias(
      RIGHT_ALIAS, RIGHT_SOURCE_SCHEMA.withMetaAndKeyColsInValue(false)
  );

  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));
  private static final ValueFormat OTHER_FORMAT = ValueFormat.of(FormatInfo.of(Format.DELIMITED));
  private final KsqlConfig ksqlConfig = new KsqlConfig(new HashMap<>());
  private StreamsBuilder builder;
  private JoinNode joinNode;

  private static final ColumnRef LEFT_JOIN_FIELD_REF = ColumnRef.of(ColumnName.of("C0"));

  private static final KeyField leftJoinField = KeyField.of(LEFT_JOIN_FIELD_REF);

  private static final Optional<WithinExpression> WITHIN_EXPRESSION =
      Optional.of(new WithinExpression(10, TimeUnit.SECONDS));

  private static final PlanNodeId nodeId = new PlanNodeId("join");
  private static final QueryContext.Stacker CONTEXT_STACKER =
      new QueryContext.Stacker().push(nodeId.toString());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private DataSource leftSource;
  @Mock
  private DataSource rightSource;
  @Mock
  private DataSourceNode left;
  @Mock
  private DataSourceNode right;
  @Mock
  private SchemaKStream<String> leftSchemaKStream;
  @Mock
  private SchemaKStream<String> rightSchemaKStream;
  @Mock
  private SchemaKTable<String> leftSchemaKTable;
  @Mock
  private SchemaKTable<String> rightSchemaKTable;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock
  private ProcessingLogContext logContext;
  @Mock
  private ProcessingLoggerFactory loggerFactory;
  @Mock
  private ProcessingLogger logger;

  @Before
  public void setUp() {
    builder = new StreamsBuilder();

    final ServiceContext serviceContext = mock(ServiceContext.class);
    when(serviceContext.getTopicClient())
        .thenReturn(mockKafkaTopicClient);

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(ksqlStreamBuilder.getServiceContext()).thenReturn(serviceContext);
    when(ksqlStreamBuilder.withKsqlConfig(any())).thenReturn(ksqlStreamBuilder);
    when(ksqlStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    when(left.getSchema()).thenReturn(LEFT_NODE_SCHEMA);
    when(right.getSchema()).thenReturn(RIGHT_NODE_SCHEMA);

    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(2);
    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(2);

    when(left.getKeyField()).thenReturn(KeyField.of(LEFT_JOIN_FIELD_REF));

    when(left.getAlias()).thenReturn(LEFT_ALIAS);
    when(right.getAlias()).thenReturn(RIGHT_ALIAS);

    when(ksqlStreamBuilder.getQueryId()).thenReturn(new QueryId("foo"));
    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(logContext);
    when(logContext.getLoggerFactory()).thenReturn(loggerFactory);
    when(loggerFactory.getLogger(any())).thenReturn(logger);

    setUpSource(left, VALUE_FORMAT, leftSource, "Foobar1");
    setUpSource(right, OTHER_FORMAT, rightSource, "Foobar2");
  }

  @Test
  public void shouldReturnLeftJoinKeyAsKeyField() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinType.LEFT,
        left,
        right,
        Optional.empty()
    );

    // Then:
    assertThat(joinNode.getKeyField().ref(), is(Optional.of(LEFT_JOIN_FIELD_REF)));
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
    assertThat(successors, equalTo(Collections.singletonList("KTABLE-SOURCE-0000000001")));
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

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Can't join T1 with T2 since the number of partitions don't match. T1 "
            + "partitions = 1; T2 partitions = 2. Please repartition either one so that the "
            + "number of partitions match."
    );

    // When:
    buildJoin(
          "SELECT t1.col0, t2.col0, t2.col1 "
              + "FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;"
    );
  }

  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKStream),
        eq(leftJoinField),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).join(
        eq(rightSchemaKStream),
        eq(leftJoinField),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).outerJoin(
        eq(rightSchemaKStream),
        eq(KeyField.none()),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldNotPerformStreamStreamJoinWithoutJoinWindow() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Stream-Stream joins must have a WITHIN clause specified. None was provided."
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldNotPerformJoinIfInputPartitionsMisMatch() {
    // Given:
    when(left.getTheSourceNode()).thenReturn(left);
    when(right.getTheSourceNode()).thenReturn(right);
    when(left.getAlias()).thenReturn(LEFT_ALIAS);
    when(right.getAlias()).thenReturn(RIGHT_ALIAS);
    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(3);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Can't join left with right since the number of partitions don't match."
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldHandleJoinIfTableHasNoKeyAndJoinFieldIsRowKey() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKTable),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKTable),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKStream).join(
        eq(rightSchemaKTable),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldNotAllowStreamToTableOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Full outer joins between streams and tables are not supported."
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldNotPerformStreamToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        Optional.of(withinExpression)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "A window definition was provided for a Stream-Table join."
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldPerformTableToTableInnerJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKTable).join(
        eq(rightSchemaKTable),
        eq(leftJoinField),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldPerformTableToTableLeftJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKTable).leftJoin(
        eq(rightSchemaKTable),
        eq(leftJoinField),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldPerformTableToTableOuterJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSchemaKTable).outerJoin(
        eq(rightSchemaKTable),
        eq(KeyField.none()),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldNotPerformTableToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    when(right.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        Optional.of(withinExpression)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "A window definition was provided for a Table-Table join."
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldHaveFullyQualifiedJoinSchema() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        Optional.empty()
    );

    // When:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.name() + "_" + "ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.name() + "_" + "ROWKEY"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.name() + "_" + "C0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(LEFT_ALIAS.name() + "_" + "L1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.name() + "_" + "ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.name() + "_" + "ROWKEY"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.name() + "_" + "C0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(RIGHT_ALIAS.name() + "_" + "R1"), SqlTypes.BIGINT)
        .build()
    ));
  }

  @Test
  public void shouldNotUseSourceSerdeOptionsForInternalTopics() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(leftSource, never()).getSerdeOptions();
    verify(rightSource, never()).getSerdeOptions();
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        WITHIN_EXPRESSION
    );

    // Then:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
        .valueColumns(LEFT_NODE_SCHEMA.value())
        .valueColumns(RIGHT_NODE_SCHEMA.value())
        .build()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void setupTable(
      final DataSourceNode node,
      final SchemaKTable<?> table
  ) {
    when(node.buildStream(ksqlStreamBuilder)).thenReturn((SchemaKTable) table);
    when(node.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
  }

  @SuppressWarnings("unchecked")
  private void setupStream(
      final DataSourceNode node,
      final SchemaKStream stream
  ) {
    when(node.buildStream(ksqlStreamBuilder)).thenReturn(stream);
    when(node.getTheSourceNode()).thenReturn(node);
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
    final SchemaKStream stream = joinNode.buildStream(ksqlStreamBuilder);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable table = (SchemaKTable) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(ksqlStreamBuilder));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(ksqlStreamBuilder));
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

  private static Optional<Column> getColumn(final LogicalSchema schema, final Predicate<ColumnName> filter) {
    return schema.value().stream()
        .filter(col -> filter.test(col.name()))
        .findFirst();
  }

  private static ColumnRef getNonKeyColumn(
      final LogicalSchema schema,
      final SourceName alias,
      final ColumnRef keyName
  ) {
    final ImmutableList<ColumnName> blackList = ImmutableList.of(
        SchemaUtil.ROWKEY_NAME,
        SchemaUtil.ROWTIME_NAME,
        keyName.name()
    );

    final Column column =
        getColumn(schema, s -> !blackList.contains(s))
            .orElseThrow(AssertionError::new);

    final Column field = schema.findValueColumn(column.ref()).get();
    return ColumnRef.of(field.name());
  }

  private static void setUpSource(
      final DataSourceNode node,
      final ValueFormat valueFormat,
      final DataSource dataSource,
      final String name
  ) {
    when(node.getDataSource()).thenReturn(dataSource);

    final KsqlTopic ksqlTopic = mock(KsqlTopic.class);
    when(ksqlTopic.getValueFormat()).thenReturn(valueFormat);
    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
  }

  private static LogicalSchema prependAlias(final SourceName alias, final LogicalSchema schema) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();
    builder.keyColumns(schema.key());
    for (final Column c : schema.value()) {
      builder.valueColumn(ColumnName.generatedJoinColumnAlias(alias, c.ref()), c.type());
    }
    return builder.build();
  }
}
