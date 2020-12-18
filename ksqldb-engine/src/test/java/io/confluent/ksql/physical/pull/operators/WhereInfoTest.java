/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
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
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.pull.operators.WhereInfo.WindowBounds;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Instant;
import java.util.Optional;
import org.junit.Test;

public class WhereInfoTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());
  private static final MetaStore METASTORE = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema MULTI_KEY_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K1"), SqlTypes.INTEGER)
      .keyColumn(ColumnName.of("K2"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("C1"), SqlTypes.INTEGER)
      .build();

  @Test
  public void shouldExtractFromLiteralEquals() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL, 
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );
    
    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(false));
    assertThat(where.getWindowBounds(), is(Optional.empty()));
  }

  @Test
  public void shouldExtractFromExpressionEquals() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, new IntegerLiteral(1))
    );

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(-1))));
    assertThat(where.isWindowed(), is(false));
    assertThat(where.getWindowBounds(), is(Optional.empty()));
  }

  @Test
  public void shouldExtractFromInExpression() {
    // Given:
    final Expression expression = new InPredicate(
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new InListExpression(
            ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))
        )
    );

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1), GenericKey.genericKey(2))));
    assertThat(where.isWindowed(), is(false));
    assertThat(where.getWindowBounds(), is(Optional.empty()));
  }

  // We should refactor the WindowBounds class to encompass the functionality around
  // extracting them from expressions instead of having them in WhereInfo
  @Test
  public void shouldExtractFromExpressionWithNoWindowBounds() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new IntegerLiteral(1)
    );

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.all()
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithGTWindowStart() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN),
            Range.all()
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithGTEWindowStart() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.downTo(Instant.ofEpochMilli(2), BoundType.CLOSED),
            Range.all()
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithLTWindowStart() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.upTo(Instant.ofEpochMilli(2), BoundType.OPEN),
            Range.all()
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithLTEWindowStart() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.upTo(Instant.ofEpochMilli(2), BoundType.CLOSED),
            Range.all()
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithGTWindowEnd() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN)
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithGTEWindowEnd() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.downTo(Instant.ofEpochMilli(2), BoundType.CLOSED)
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithLTWindowEnd() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.upTo(Instant.ofEpochMilli(2), BoundType.OPEN)
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithLTEWindowEnd() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.upTo(Instant.ofEpochMilli(2), BoundType.CLOSED)
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithEQWindowEnd() {
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
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.all(),
            Range.singleton(Instant.ofEpochMilli(2))
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithBothWindowBounds() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.downTo(Instant.ofEpochMilli(2), BoundType.OPEN),
            Range.upTo(Instant.ofEpochMilli(3), BoundType.CLOSED)
        )
    )));
  }

  @Test
  public void shouldExtractFromExpressionWithGTWindowStartText() {
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1))));
    assertThat(where.isWindowed(), is(true));
    assertThat(where.getWindowBounds(), is(Optional.of(
        new WindowBounds(
            Range.downTo(Instant.ofEpochMilli(1577836800_000L), BoundType.OPEN),
            Range.all()
        )
    )));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported WINDOWEND bounds: [IS_DISTINCT_FROM]."));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("(WINDOWSTART = 2)` cannot be combined with other WINDOWSTART bounds"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate WINDOWSTART bounds on: GREATER_THAN"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Duplicate WINDOWSTART bounds on: GREATER_THAN"));
  }

  @Test
  public void shouldSupportMultiKeyExpressions() {
    // Given:
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

    // When:
    final WhereInfo where = WhereInfo.extractWhereInfo(expression, MULTI_KEY_SCHEMA, true, METASTORE, CONFIG);

    // Then:
    assertThat(where.getKeysBound(), is(ImmutableList.of(GenericKey.genericKey(1, 2))));
  }


  @Test
  public void shouldNotSupportMultiKeyExpressionsThatDontCoverAllKeys() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K1")),
        new IntegerLiteral(1)
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> WhereInfo.extractWhereInfo(expression, MULTI_KEY_SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString(
        "Multi-column sources must specify every key in the WHERE clause. "
            + "Specified: [`K1`] Expected: [`K1` INTEGER KEY, `K2` INTEGER KEY]"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Multiple bounds on key column."));
  }

  @Test
  public void shouldThrowIfNotComparisonExpression() {
    // Given:
    final Expression expression = new BooleanLiteral(true);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported expression: true."));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("WHERE clause on non-key column: C1"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Bound on key columns '[`K` INTEGER KEY]' must currently be '='"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("'foo' can not be converted to the type of the key column: K INTEGER KEY"));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Cannot use WINDOWSTART/WINDOWEND on non-windowed source."));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse datetime: Foobar."));
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
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, true, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Window bounds must be an INT, BIGINT or STRING containing a datetime."));
  }

  @Test
  public void shouldExtractFromNullLiteral() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new NullLiteral()
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> WhereInfo.extractWhereInfo(expression, SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Primary key columns can not be NULL: (K = null)"));
  }

  @Test
  public void shouldThrowOnInExpressionWithMultiColKeySchema() {
    // Given:
    final Expression expression = new InPredicate(
        new UnqualifiedColumnReferenceExp(ColumnName.of("K")),
        new InListExpression(
            ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))
        )
    );

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> WhereInfo.extractWhereInfo(expression, MULTI_KEY_SCHEMA, false, METASTORE, CONFIG));

    // Then:
    assertThat(e.getMessage(), containsString("Schemas with multiple KEY columns are not supported for IN predicates"));
  }

}