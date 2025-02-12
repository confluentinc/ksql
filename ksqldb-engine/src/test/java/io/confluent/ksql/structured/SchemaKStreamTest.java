/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.UserRepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKStreamTest {

  private static final ColumnName KEY = ColumnName.of("Bob");

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture
      .getNewMetaStore(new InternalFunctionRegistry());
  private final KeyFormat keyFormat = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of());
  private final KeyFormat windowedKeyFormat = KeyFormat
      .windowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of(), WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty()));
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()),
      SerdeFeatures.of());
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");

  private SchemaKStream initialSchemaKStream;
  private SchemaKTable schemaKTable;
  private SchemaKStream schemaKStream;
  private KsqlStream<?> ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private StepSchemaResolver schemaResolver;

  @Mock
  private ExecutionStep tableSourceStep;
  @Mock
  private ExecutionStep streamSourceStep;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private PlanBuildContext buildContext;
  @Mock
  private KsqlTopic topic;
  @Mock
  private FormatInfo internalFormats;

  @Before
  @SuppressWarnings("rawtypes")
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    schemaResolver = new StepSchemaResolver(ksqlConfig, functionRegistry);
    ksqlStream = (KsqlStream) metaStore.getSource(SourceName.of("TEST1"));
    final KsqlTable<?> ksqlTable = (KsqlTable) metaStore.getSource(SourceName.of("TEST2"));
    schemaKTable = new SchemaKTable(
        tableSourceStep,
        ksqlTable.getSchema(),
        keyFormat,
        ksqlConfig,
        functionRegistry);
    schemaKStream = new SchemaKStream(
        streamSourceStep,
        ksqlStream.getSchema(),
        keyFormat,
        ksqlConfig,
        functionRegistry);
  }

  @Test
  public void shouldBuildSchemaForSelect() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0 AS K, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream<?> projectedSchemaKStream = initialSchemaKStream.select(
        ImmutableList.of(ColumnName.of("K")),
        selectExpressions,
        childContextStacker,
        buildContext,
        internalFormats);

    // Then:
    assertThat(
        projectedSchemaKStream.getSchema(),
        is(schemaResolver.resolve(
            projectedSchemaKStream.getSourceStep(), initialSchemaKStream.schema))
    );
  }

  @Test
  public void shouldNotRepartitionIfSameKeyField() {
    // Given:
    givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY col0 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> result = initialSchemaKStream
        .selectKey(
            valueFormat.getFormatInfo(),
            ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))),
            Optional.empty(),
            childContextStacker,
            false
        );

    // Then:
    assertThat(result, is(initialSchemaKStream));
  }

  @Test
  public void shouldFailIfForceRepartitionWindowedStream() {
    // Given:
    givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY col0 EMIT CHANGES;",
        windowedKeyFormat);

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () -> initialSchemaKStream
        .selectKey(
            valueFormat.getFormatInfo(),
            ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))),
            Optional.empty(),
            childContextStacker,
            true)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Implicit repartitioning of windowed sources is not supported. See https://github.com/confluentinc/ksql/issues/4385."));
  }

  @Test
  public void shouldRepartitionIfForced() {
    // Given:
    givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY col0 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))),
        Optional.empty(),
        childContextStacker,
        true
    );

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamSelectKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL0")))
            )
        )
    );
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnRepartitionByMissingField() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY not_here EMIT CHANGES;");
    final UserRepartitionNode repartitionNode = (UserRepartitionNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    initialSchemaKStream.selectKey(valueFormat.getFormatInfo(), repartitionNode.getPartitionBys(),
        Optional.empty(), childContextStacker, false
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldFailRepartitionTable() {
    // Given:
    givenInitialKStreamOf("SELECT * FROM test2 EMIT CHANGES;");

    final UnqualifiedColumnReferenceExp col2 =
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL2"));

    // When:
    schemaKTable.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(col2),
        Optional.empty(),
        childContextStacker,
        false
    );
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void shouldRewriteTimeComparisonInFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 "
            + "WHERE ROWTIME = '1984-01-01T00:00:00+00:00' EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream<?> filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final StreamFilter step = (StreamFilter) filteredSchemaKStream.getSourceStep();
    assertThat(
        step.getFilterExpression(),
        equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new UnqualifiedColumnReferenceExp(ColumnName.of("ROWTIME")),
                new LongLiteral(441763200000L)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream<?> filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamFilter(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))),
        Optional.empty(),
        childContextStacker,
        false
    );

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamSelectKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1")))
            )
        )
    );
  }

  @Test
  public void shouldBuildSchemaForSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        valueFormat.getFormatInfo(),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))),
        Optional.empty(),
        childContextStacker,
        false
    );

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSchema(),
        is(schemaResolver.resolve(
            rekeyedSchemaKStream.getSourceStep(), initialSchemaKStream.getSchema()))
    );
  }

  @Test
  public void shouldBuildStepForGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat.getFormatInfo(),
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupByKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats
                    .of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(), SerdeFeatures.of(),
                        SerdeFeatures.of())
            )
        )
    );
  }

  @Test
  public void shouldBuildSchemaForGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat.getFormatInfo(),
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(
        groupedSchemaKStream.schema,
        is(schemaResolver.resolve(
            groupedSchemaKStream.getSourceStep(), initialSchemaKStream.getSchema()))
    );
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
            new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat.getFormatInfo(),
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupBy(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats
                    .of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(), SerdeFeatures.of(),
                        SerdeFeatures.of()),
                groupBy
            )
        )
    );
  }

  @Test
  public void shouldBuildSchemaForGroupBy() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat.getFormatInfo(),
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKStream.schema, is(schemaResolver.resolve(
        groupedSchemaKStream.getSourceStep(), initialSchemaKStream.getSchema()))
    );
  }

  @FunctionalInterface
  @SuppressWarnings("rawtypes")
  private interface StreamStreamJoin {
    SchemaKStream join(
        SchemaKStream otherSchemaKStream,
        ColumnName keyNameCol,
        WithinExpression withinExpression,
        FormatInfo leftFormat,
        FormatInfo rightFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @FunctionalInterface
  @SuppressWarnings("rawtypes")
  private interface StreamTableJoin {
    SchemaKStream join(
        SchemaKTable other,
        ColumnName keyNameCol,
        FormatInfo leftFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  @SuppressWarnings({"rawtypes", "deprecation"}) // can be fixed after GRACE clause is made mandatory
  public void shouldBuildStepForStreamStreamJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream);

    final List<Pair<JoinType, StreamStreamJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.OUTER, initialSchemaKStream::outerJoin),
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::innerJoin)
    );

    final JoinWindows joinWindows = JoinWindows.of(Duration.ofSeconds(1));
    final WindowTimeClause grace = new WindowTimeClause(5, TimeUnit.SECONDS);
    final WithinExpression withinExpression = new WithinExpression(1, TimeUnit.SECONDS, grace);

    for (final Pair<JoinType, StreamStreamJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKStream,
          KEY,
          withinExpression,
          valueFormat.getFormatInfo(),
          valueFormat.getFormatInfo(),
          childContextStacker
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamStreamJoin(
                  childContextStacker,
                  testcase.left,
                  KEY,
                  InternalFormats.of(keyFormat, valueFormat.getFormatInfo()),
                  InternalFormats.of(keyFormat, valueFormat.getFormatInfo()),
                  initialSchemaKStream.getSourceStep(),
                  schemaKStream.getSourceStep(),
                  joinWindows,
                  Optional.of(grace)
              )
          )
      );
    }
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void shouldBuildStepForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::innerJoin)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          KEY,
          valueFormat.getFormatInfo(),
          childContextStacker
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamTableJoin(
                  childContextStacker,
                  testcase.left,
                  KEY,
                  Formats.of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(),
                      SerdeFeatures.of(), SerdeFeatures.of()),
                  initialSchemaKStream.getSourceStep(),
                  schemaKTable.getSourceTableStep()
              )
          )
      );
    }
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void shouldBuildSchemaForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::innerJoin)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          KEY,
          valueFormat.getFormatInfo(),
          childContextStacker
      );

      // Then:
      assertThat(joinedKStream.getSchema(), is(schemaResolver.resolve(
          joinedKStream.getSourceStep(),
          initialSchemaKStream.getSchema(),
          schemaKTable.getSchema()))
      );
    }
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void shouldThrowOnIntoIfKeyFormatWindowInfoIsDifferent() {
    // Given:
    final SchemaKStream stream = new SchemaKStream(
        sourceStep,
        ksqlStream.getSchema(),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );

    when(topic.getKeyFormat()).thenReturn(KeyFormat.windowed(
        keyFormat.getFormatInfo(),
        SerdeFeatures.of(),
        WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty())
    ));

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> stream.into(topic, childContextStacker, Optional.empty())
    );
  }

  @SuppressWarnings("rawtypes")
  private SchemaKStream buildSchemaKStream(
      final LogicalSchema schema,
      final ExecutionStep sourceStep
  ) {
    return new SchemaKStream(
        sourceStep,
        schema,
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("rawtypes")
  private LogicalSchema buildJoinSchema(final KsqlStream stream) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();
    builder.keyColumns(stream.getSchema().key());
    for (final Column c : stream.getSchema().value()) {
      builder.valueColumn(ColumnNames.generatedJoinColumnAlias(stream.getName(), c.name()), c.type());
    }
    return builder.build();
  }

  @SuppressWarnings("rawtypes")
  private SchemaKStream buildSchemaKStreamForJoin(final KsqlStream ksqlStream) {
    return buildSchemaKStream(
        buildJoinSchema(ksqlStream),
        sourceStep
    );
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
    return givenInitialKStreamOf(selectQuery, keyFormat);
  }

  @SuppressWarnings("rawtypes")
  private PlanNode givenInitialKStreamOf(final String selectQuery, final KeyFormat keyFormat) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        ksqlConfig,
        selectQuery,
        metaStore
    );
    initialSchemaKStream = new SchemaKStream(
        sourceStep,
        logicalPlan.getLeftmostSourceNode().getSchema(),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );

    return logicalPlan;
  }
}
