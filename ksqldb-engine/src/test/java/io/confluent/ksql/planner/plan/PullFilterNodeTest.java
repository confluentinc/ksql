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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.plan.PullFilterNode.WindowBounds;
import io.confluent.ksql.planner.plan.PullFilterNode.WindowBounds.WindowRange;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.INTEGER)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(K, SqlTypes.INTEGER)
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
  }

  @Test
  public void shouldExtractKeyValueFromLiteralEquals() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        false
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(filterNode.getWindowBounds(), is(Optional.empty()));
  }

  @Test
  public void shouldExtractKeyValueFromNullLiteral() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new NullLiteral()
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Primary key columns can not be NULL: (K = null)"));
  }


  @Test
  public void shouldExtractKeyValueFromExpressionEquals() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, new IntegerLiteral(1))
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        false
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();

    // Then:
    assertThat(keys, is(ImmutableList.of(-1)));
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(filterNode.getWindowBounds(), is(Optional.empty()));
  }

  @Test
  public void shouldExtractKeyValuesFromInExpression() {
    // Given:
    final Expression expression = new InPredicate(
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new InListExpression(
            ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))
        )
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        false
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();

    // Then:
    assertThat(keys, is(ImmutableList.of(1,2)));
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(filterNode.getWindowBounds(), is(Optional.empty()));
  }

  // We should refactor the WindowBounds class to encompass the functionality around
  // extracting them from expressions instead of having them in WhereInfo
  @Test
  public void shouldExtractKeyValueFromExpressionWithNoWindowBounds() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(filterNode.getWindowBounds(), is(Optional.of(
        new WindowBounds()
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBountsFromExpressionWithGTWindowStart() {
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
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, null, Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN)),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithGTEWindowStart() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, null, Range.downTo(Instant.ofEpochMilli(2), BoundType.CLOSED)),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithLTWindowStart() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.LESS_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, Range.upTo(Instant.ofEpochMilli(2), BoundType.OPEN), null),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithLTEWindowStart() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.LESS_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, Range.upTo(Instant.ofEpochMilli(2), BoundType.CLOSED), null),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithGTWindowEnd() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(),
            new WindowRange(
              null, null, Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN))
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithGTEWindowEnd() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(),
            new WindowRange(
                null, null, Range.downTo(Instant.ofEpochMilli(2), BoundType.CLOSED))
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithLTWindowEnd() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.LESS_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(),
            new WindowRange(
                null, Range.upTo(Instant.ofEpochMilli(2), BoundType.OPEN), null)
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithLTEWindowEnd() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.LESS_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(),
            new WindowRange(
                null, Range.upTo(Instant.ofEpochMilli(2), BoundType.CLOSED), null)
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithEQWindowEnd() {
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
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(),
            new WindowRange(
                Range.singleton(Instant.ofEpochMilli(2)), null, null)
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithBothWindowBounds() {
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
        Type.LESS_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWEND")),
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
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null,
                null,
                Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN)),
            new WindowRange(
                null,
                Range.upTo(Instant.ofEpochMilli(3), BoundType.CLOSED),
                null)
        )
    )));
  }

  @Test
  public void shouldExtractKeyValueAndWindowBoundsFromExpressionWithGTWindowStartText() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new StringLiteral("2020-01-01")
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        keyExp,
        windowStart
    );
    PullFilterNode filterNode = new PullFilterNode(
        NODE_ID,
        source,
        expression,
        analysis,
        metaStore,
        ksqlConfig,
        true
    );

    // When:
    final List<Object> keys = filterNode.getKeyValues();
    final Optional<WindowBounds> windowBounds = filterNode.getWindowBounds();

    // Then:
    assertThat(keys, is(ImmutableList.of(1)));
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(windowBounds, is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, null, Range.downTo(Instant.ofEpochMilli(1577836800_000L), BoundType.OPEN)),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldThrowIfComparisonOnNonKey() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("C1")),
        new IntegerLiteral(1)
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("WHERE clause on unsupported column: C1"));
  }

  @Test
  public void shouldThrowIfNotComparisonExpression() {
    // Given:
    final Expression expression = new BooleanLiteral(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported expression in WHERE clause: true."));
  }


  @Test
  public void shouldThrowIfNonEqualComparison() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Bound on 'K' must currently be '='."));
  }

  @Test
  public void shouldThrowOnInAndComparisonExpression() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression expression2 = new InPredicate(
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new InListExpression(
            ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))
        )
    );
    final Expression expression  = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expression1,
        expression2
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("The IN predicate cannot be combined with other comparisons"));
  }

  @Test
  public void shouldThrowIfNonConvertibleType() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new StringLiteral("foo")
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("'foo' can not be converted to the type of the key column: K INTEGER KEY"));
  }

  @Test
  public void shouldThrowOnMultiColKeySchema() {
    // Given:
    final LogicalSchema multiSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("K"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
        .build();
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, new IntegerLiteral(1))
    );
    when(source.getSchema()).thenReturn(multiSchema);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Schemas with multiple KEY columns are not supported"));
  }

  @Test
  public void shouldThrowOnMultiKeyExpressions() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression expression2 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(2)
    );
    final Expression expression  = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expression1,
        expression2
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("An equality condition on the key column cannot be combined with other comparisons"));
  }

  @Test
  public void shouldThrowOnFailToParseTimestamp() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new StringLiteral("Foobar")
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse timestamp 'Foobar'"));
  }

  @Test
  public void shouldThrowOnInvalidTimestampType() {
    // Given:
    final Expression keyExp = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression windowStart = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new BooleanLiteral(false)
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
            ksqlConfig,
            true
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Window bounds must be an INT, BIGINT or STRING containing a datetime."));
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

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            true
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

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            true
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

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            true
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

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new PullFilterNode(
            NODE_ID,
            source,
            expression,
            analysis,
            metaStore,
            ksqlConfig,
            true
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
            ksqlConfig,
            false
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot use WINDOWSTART/WINDOWEND on non-windowed source."));
  }

}