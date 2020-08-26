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

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.RepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import java.util.Collections;
import java.util.List;
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
      .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()));
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()));
  private final ValueFormat rightFormat = ValueFormat.of(FormatInfo.of(FormatFactory.DELIMITED.name()));
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");

  private SchemaKStream initialSchemaKStream;
  private SchemaKTable schemaKTable;
  private KsqlStream<?> ksqlStream;
  private InternalFunctionRegistry functionRegistry;
  private StepSchemaResolver schemaResolver;

  @Mock
  private ExecutionStep tableSourceStep;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KsqlQueryBuilder queryBuilder;

  @Before
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
        queryBuilder);

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
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY col0 EMIT CHANGES;");
    final RepartitionNode repartitionNode = (RepartitionNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream<?> result = initialSchemaKStream
        .selectKey(repartitionNode.getPartitionBy(), childContextStacker);

    // Then:
    assertThat(result, is(initialSchemaKStream));
  }

  @Test
  public void shouldNotRepartitionIfRowkey() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY col0 EMIT CHANGES;");
    final RepartitionNode repartitionNode = (RepartitionNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream<?> result = initialSchemaKStream
        .selectKey(repartitionNode.getPartitionBy(), childContextStacker);

    // Then:
    assertThat(result, is(initialSchemaKStream));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnRepartitionByMissingField() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 PARTITION BY not_here EMIT CHANGES;");
    final RepartitionNode repartitionNode = (RepartitionNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    initialSchemaKStream.selectKey(repartitionNode.getPartitionBy(),
        childContextStacker
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldFailRepartitionTable() {
    // Given:
    givenInitialKStreamOf("SELECT * FROM test2 EMIT CHANGES;");

    final UnqualifiedColumnReferenceExp col2 =
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL2"));

    // When:
    schemaKTable.selectKey(col2, childContextStacker);
  }

  @Test
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
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL1")),
        childContextStacker);

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamSelectKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                new UnqualifiedColumnReferenceExp(ColumnName.of("COL1"))
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
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL1")),
        childContextStacker);

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
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupByKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOptions.of())
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
        valueFormat,
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
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupBy(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOptions.of()),
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
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKStream.schema, is(schemaResolver.resolve(
        groupedSchemaKStream.getSourceStep(), initialSchemaKStream.getSchema()))
    );
  }

  @FunctionalInterface
  private interface StreamStreamJoin {
    SchemaKStream join(
        SchemaKStream otherSchemaKStream,
        JoinWindows joinWindows,
        ValueFormat leftFormat,
        ValueFormat rightFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @FunctionalInterface
  private interface StreamTableJoin {
    SchemaKStream join(
        SchemaKTable other,
        ColumnName keyNameCol,
        ValueFormat leftFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          KEY,
          valueFormat,
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
                  Formats.of(keyFormat, valueFormat, SerdeOptions.of()),
                  initialSchemaKStream.getSourceStep(),
                  schemaKTable.getSourceTableStep()
              )
          )
      );
    }
  }

  @Test
  public void shouldBuildSchemaForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(ksqlStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          KEY,
          valueFormat,
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

  private LogicalSchema buildJoinSchema(final KsqlStream stream) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();
    builder.keyColumns(stream.getSchema().key());
    for (final Column c : stream.getSchema().value()) {
      builder.valueColumn(ColumnNames.generatedJoinColumnAlias(stream.getName(), c.name()), c.type());
    }
    return builder.build();
  }

  private SchemaKStream buildSchemaKStreamForJoin(final KsqlStream ksqlStream) {
    return buildSchemaKStream(
        buildJoinSchema(ksqlStream),
        sourceStep
    );
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
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
