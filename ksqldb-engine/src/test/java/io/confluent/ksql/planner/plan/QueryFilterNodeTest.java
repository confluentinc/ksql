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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.BytesLiteral;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.QueryFilterNode.WindowBounds;
import io.confluent.ksql.planner.plan.QueryFilterNode.WindowBounds.WindowRange;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryFilterNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName COL0 = ColumnName.of("COL0");

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.INTEGER)
      .valueColumn(COL0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(K, SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema MULTI_KEY_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema STRING_SCHEMA = LogicalSchema.builder()
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
  private QueryPlannerOptions plannerOptions;

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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        false,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.empty()));
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        false,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(-1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.empty()));
  }

  @Test
  public void shouldExtractKeyValueFromExpressionEquals_multipleDisjuncts() {
    // Given:
    final Expression keyExp1 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    final Expression keyExp2 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        keyExp1,
        keyExp2
    );
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        false,
        plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(keys.size(), is(2));
    final KeyConstraint keyConstraint1 = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint1.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint1.getWindowBounds(), is(Optional.empty()));
    final KeyConstraint keyConstraint2 = (KeyConstraint) keys.get(1);
    assertThat(keyConstraint2.getKey(), is(GenericKey.genericKey(2)));
    assertThat(keyConstraint2.getWindowBounds(), is(Optional.empty()));
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        false,
        plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(false));
    assertThat(keys.size(), is(2));
    final KeyConstraint keyConstraint0 = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint0.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint0.getWindowBounds(), is(Optional.empty()));
    final KeyConstraint keyConstraint1 = (KeyConstraint) keys.get(1);
    assertThat(keyConstraint1.getKey(), is(GenericKey.genericKey(2)));
    assertThat(keyConstraint1.getWindowBounds(), is(Optional.empty()));
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(new WindowBounds())));
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
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
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(true));
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1)));
    assertThat(keyConstraint.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            new WindowRange(
                null, null, Range.downTo(Instant.ofEpochMilli(1577836800_000L), BoundType.OPEN)),
            new WindowRange()
        )
    )));
  }

  @Test
  public void shouldSupportMultiKeyExpressions() {
    // Given:
    when(source.getSchema()).thenReturn(MULTI_KEY_SCHEMA);
    final Expression expression1 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
        new IntegerLiteral(1)
    );
    final Expression expression2 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K2")),
        new IntegerLiteral(2)
    );
    final Expression expression  = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        expression1,
        expression2
    );
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        true,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1, 2)));
  }

  @Test
  public void shouldThrowKeyExpressionThatDoestCoverKey() {
    // Given:
    when(source.getSchema()).thenReturn(INPUT_SCHEMA);
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(1234)
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("WHERE clause missing key column for disjunct: "
        + "(WINDOWSTART = 1234)"));
  }

  @Test
  public void shouldExtractConstraintForSpecialCol_tableScan() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
    when(source.getSchema()).thenReturn(INPUT_SCHEMA);
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(1234)
    );

    // Then:
    expectTableScan(expression, true);
  }

  @Test
  public void shouldThrowKeyExpressionThatDoestCoverKey_multipleDisjuncts() {
    // Given:
    when(source.getSchema()).thenReturn(INPUT_SCHEMA);
    final Expression keyExp1 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("WINDOWSTART")),
        new IntegerLiteral(1)
    );
    final Expression keyExp2 = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(2)
    );
    final Expression expression = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        keyExp1,
        keyExp2
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("WHERE clause missing key column for disjunct: "
        + "(WINDOWSTART = 1)"));
  }

  @Test
  public void shouldThrowMultiKeyExpressionsThatDontCoverAllKeys() {
    // Given:
    when(source.getSchema()).thenReturn(MULTI_KEY_SCHEMA);
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
        new IntegerLiteral(1)
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions));

    // Then:
    assertThat(e.getMessage(), containsString(
        "Multi-column sources must specify every key in the WHERE clause. "
            + "Specified: [`K1`] Expected: [`K1` INTEGER KEY, `K2` INTEGER KEY]"));
  }

  @Test
  public void shouldExtractConstraintForMultiKeyExpressionsThatDontCoverAllKeys_tableScan() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
    when(source.getSchema()).thenReturn(MULTI_KEY_SCHEMA);
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
        new IntegerLiteral(1)
    );

    // Then:
    expectTableScan(expression, false);
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Bound on non-existent column `C1`."));
  }

  @Test
  public void shouldThrowIfNotComparisonExpression() {
    // Given:
    final Expression expression = new BooleanLiteral(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported expression in WHERE clause: true."));
  }

  @Test
  public void shouldExtractKeyFromNonEqualComparison() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );

    final QueryFilterNode filterNode = new QueryFilterNode(
      NODE_ID,
      source,
      expression,
      metaStore,
      ksqlConfig,
      false,
      plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), is(instanceOf(NonKeyConstraint.class)));
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));



    // Then:
    assertThat(e.getMessage(), containsString("A comparison condition on the key column cannot be "
        + "combined with other comparisons"));
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Multi-column sources must specify every key in the WHERE clause. Specified: [`K`] Expected: [`K` INTEGER KEY, `K2` INTEGER KEY]."));
  }

  @Test
  public void shouldThrowOnMultipleKeyExpressions() {
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("A comparison condition on the key column cannot be combined with other comparisons"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldExtractConstraintWithMultipleKeyExpressions_tableScan() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
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

    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
    );

    // Then:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
  }

  @Test
  public void shouldThrowOnNonKeyCol() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0")),
        new StringLiteral("abc")
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("WHERE clause missing key column for disjunct: "
        + "(COL0 = 'abc')"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldExtractConstraintWithNonKeyCol_tableScan() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0")),
        new StringLiteral("abc")
    );

    // Then:
    expectTableScan(expression, false);
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Window bounds must resolve to an INT, BIGINT, or "
        + "STRING containing a datetime."));
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            true,
            plannerOptions
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
        () -> new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
        ));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot use WINDOWSTART/WINDOWEND on non-windowed source."));
  }

  @Test
  public void shouldRangeScanFromStringRangeComparison() {
    // Given:
    when(source.getSchema()).thenReturn(STRING_SCHEMA);
    final Expression expression = new ComparisonExpression(
      Type.GREATER_THAN,
      new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
      new StringLiteral("1")
    );

    // Then:
    expectRangeScan(expression, false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldTableScanFromIntRangeComparison() {
    // Given:
    final Expression expression = new ComparisonExpression(
      Type.GREATER_THAN,
      new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
      new IntegerLiteral(1)
    );

    // Then:
    expectTableScan(expression, false);
  }

  @Test
  public void shouldSupportMultiRangeExpressionsUsingTableScan() {
    // Given:
    when(source.getSchema()).thenReturn(MULTI_KEY_SCHEMA);
    final Expression expression1 = new ComparisonExpression(
      Type.GREATER_THAN,
      new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
      new IntegerLiteral(1)
    );
    final Expression expression2 = new ComparisonExpression(
      Type.GREATER_THAN,
      new UnqualifiedColumnReferenceExp(ColumnName.of("K2")),
      new IntegerLiteral(2)
    );
    final Expression expression  = new LogicalBinaryExpression(
      LogicalBinaryExpression.Type.AND,
      expression1,
      expression2
    );

    expectTableScan(expression, false);
  }

  @SuppressWarnings("unchecked")
  private void expectTableScan(final Expression expression, final boolean windowed) {
    // Given:
    QueryFilterNode filterNode = new QueryFilterNode(
        NODE_ID,
        source,
        expression,
        metaStore,
        ksqlConfig,
        windowed,
        plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(windowed));
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), isA((Class) NonKeyConstraint.class));
  }

  @SuppressWarnings("unchecked")
  private void expectRangeScan(final Expression expression, final boolean windowed) {
    // Given:
    QueryFilterNode filterNode = new QueryFilterNode(
      NODE_ID,
      source,
      expression,
      metaStore,
      ksqlConfig,
      windowed,
      plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(filterNode.isWindowed(), is(windowed));
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), isA((Class) KeyConstraint.class));
    assertThat(((KeyConstraint) keys.get(0)).isRangeOperator(), is(true));
  }

  @Test
  public void shouldThrowNonStringForLike() {
    // Given:
    final Expression expression = new LikePredicate(
            new StringLiteral("a"),
            new IntegerLiteral(10),
            Optional.empty());

    // When:
    final KsqlException e = assertThrows(
            KsqlException.class,
            () -> new QueryFilterNode(
                    NODE_ID,
                    source,
                    expression,
                    metaStore,
                    ksqlConfig,
                    false,
                    plannerOptions
            ));

    // Then:
    assertThat(e.getMessage(), containsString("Like condition must be between strings"));
  }

  @Test
  public void shouldThrowNotKeyColumnForBetween() {
    // Given:
    final Expression expression = new BetweenPredicate(
            new StringLiteral("a"),
            new StringLiteral("b"),
            new IntegerLiteral(10)
    );

    // When:
    final KsqlException e = assertThrows(
            KsqlException.class,
            () -> new QueryFilterNode(
                    NODE_ID,
                    source,
                    expression,
                    metaStore,
                    ksqlConfig,
                    false,
                    plannerOptions
            ));

    // Then:
    assertThat(e.getMessage(), containsString("A comparison must directly reference a key column"));
  }

  @Test
  public void shouldReturnNonKeyConstraintIntGreater() {
    // Given:
    final Expression expression = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
            new IntegerLiteral(1)
    );
    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(NonKeyConstraint.class));
  }

  @Test
  public void shouldReturnKeyConstraintInt() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
    final Expression keyExp1 = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
            new IntegerLiteral(1)
    );
    final Expression keyExp2 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
            new IntegerLiteral(3)
    );
    final Expression expression = new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            keyExp1,
            keyExp2
    );
    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(3)));
    assertThat(keyConstraint.getOperator(), is(KeyConstraint.ConstraintOperator.EQUAL));
  }

  @Test
  public void shouldExtractMultiColKeySchema() {
    // Given:
    final LogicalSchema multiSchema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
            .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
            .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
            .build();
    final Expression keyExp1 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new IntegerLiteral(1)
    );
    final Expression keyExp2 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K2")),
            new IntegerLiteral(3)
    );
    final Expression expression = new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            keyExp1,
            keyExp2
    );

    when(source.getSchema()).thenReturn(multiSchema);

    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey(1, 3)));
    assertThat(keyConstraint.getOperator(), is(KeyConstraint.ConstraintOperator.EQUAL));
  }

  @Test
  public void shouldNotExtractMultiColKeySchema() {
    // Given:
    final LogicalSchema multiSchema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
            .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
            .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
            .build();
    final Expression keyExp1 = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new IntegerLiteral(1)
    );
    final Expression keyExp2 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K2")),
            new IntegerLiteral(3)
    );
    final Expression expression = new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            keyExp1,
            keyExp2
    );

    when(source.getSchema()).thenReturn(multiSchema);

    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(NonKeyConstraint.class));
  }

  @Test
  public void shouldReturnKeyConstraintStringGreater() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
            .build();
    when(source.getSchema()).thenReturn(schema);
    final Expression expression = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new StringLiteral("v1")
    );
    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions
    );

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey("v1")));
    assertThat(keyConstraint.getOperator(), is(KeyConstraint.ConstraintOperator.GREATER_THAN));
  }

  @Test
  public void shouldReturnKeyConstraintString() {
    // Given:
    when(plannerOptions.getTableScansEnabled()).thenReturn(true);
    final LogicalSchema schema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
            .build();
    when(source.getSchema()).thenReturn(schema);
    final Expression keyExp1 = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new StringLiteral("v1")
    );
    final Expression keyExp2 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new StringLiteral("v2")
    );
    final Expression expression = new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.AND,
            keyExp1,
            keyExp2
    );
    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(1));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint.getKey(), is(GenericKey.genericKey("v2")));
    assertThat(keyConstraint.getOperator(), is(KeyConstraint.ConstraintOperator.EQUAL));
  }

  @Test
  public void shouldReturnKeyConstraintStringForOr() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
            .build();
    when(source.getSchema()).thenReturn(schema);
    final Expression keyExp1 = new ComparisonExpression(
            Type.GREATER_THAN,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new StringLiteral("v1")
    );
    final Expression keyExp2 = new ComparisonExpression(
            Type.EQUAL,
            new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
            new StringLiteral("v2")
    );
    final Expression expression = new LogicalBinaryExpression(
            LogicalBinaryExpression.Type.OR,
            keyExp1,
            keyExp2
    );
    QueryFilterNode filterNode = new QueryFilterNode(
            NODE_ID,
            source,
            expression,
            metaStore,
            ksqlConfig,
            false,
            plannerOptions);

    // When:
    final List<LookupConstraint> keys = filterNode.getLookupConstraints();

    // Then:
    assertThat(keys.size(), is(2));
    assertThat(keys.get(0), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint1 = (KeyConstraint) keys.get(0);
    assertThat(keyConstraint1.getKey(), is(GenericKey.genericKey("v1")));
    assertThat(keyConstraint1.getOperator(), is(KeyConstraint.ConstraintOperator.GREATER_THAN));
    assertThat(keys.get(1), instanceOf(KeyConstraint.class));
    final KeyConstraint keyConstraint2 = (KeyConstraint) keys.get(1);
    assertThat(keyConstraint2.getKey(), is(GenericKey.genericKey("v2")));
    assertThat(keyConstraint2.getOperator(), is(KeyConstraint.ConstraintOperator.EQUAL));
  }
}