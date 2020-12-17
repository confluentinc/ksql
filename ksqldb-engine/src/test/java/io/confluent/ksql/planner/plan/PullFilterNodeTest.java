/*
 * Copyright 2020 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullFilterNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName ALIAS = ColumnName.of("GRACE");

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.STRING)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(K, SqlTypes.STRING)
      .build();

  @Mock
  private PlanNode source;
  @Mock
  private MetaStore metaStore;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private RewrittenAnalysis analysis;
  @Mock
  private Analysis.AliasedDataSource aliasedDataSource;
  @Mock
  private DataSource dataSource;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private KeyFormat keyFormat;


  @Before
  public void setUp() {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getSchema()).thenReturn(INPUT_SCHEMA);
    when(analysis.getFrom()).thenReturn(aliasedDataSource);
    when(aliasedDataSource.getDataSource()).thenReturn(dataSource);
    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
  }

  @Test
  public void shouldThrowOnEqOneWindowBoundAndGtAnother() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression windowEnd = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(3)
    );

    final Expression expressionA = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expressionA,
        windowEnd
    );
    when(keyFormat.isWindowed()).thenReturn(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig
        ));

    // Then:
    assertThat(e.getMessage(), containsString("(WINDOWSTART > 3)` cannot be combined with other `WINDOWSTART` bounds"));
  }

  @Test
  public void shouldThrowOnGTAndGTEComparisonsForSameBound() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression windowEnd = new ComparisonExpression(
        Type.GREATER_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(3)
    );

    final Expression expressionA = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expressionA,
        windowEnd
    );
    when(keyFormat.isWindowed()).thenReturn(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate `WINDOWSTART` bounds on: GREATER_THAN"));
  }

  @Test
  public void shouldThrowOnInvalidTypeComparisonForWindowBound() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.IS_DISTINCT_FROM,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    when(keyFormat.isWindowed()).thenReturn(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported `WINDOWEND` bounds: IS_DISTINCT_FROM."));
  }

  @Test
  public void shouldThrowOnTwoGTComparisonsForSameBound() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression windowEnd = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(3)
    );

    final Expression expressionA = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expressionA,
        windowEnd
    );
    when(keyFormat.isWindowed()).thenReturn(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate `WINDOWSTART` bounds on: GREATER_THAN"));
  }

  @Test
  public void shouldThrowOnUsageOfWindowBoundOnNonwindowedTable() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot use WINDOWSTART/WINDOWEND on non-windowed source."));
  }

}