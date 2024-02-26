/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine.rewrite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DateLiteral;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IntervalUnit;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionTreeRewriterTest {

  private static final List<Expression> LITERALS = ImmutableList.of(
      new IntegerLiteral(1),
      new LongLiteral(1),
      new DoubleLiteral(1.0),
      new BooleanLiteral("true"),
      new StringLiteral("abcd"),
      new NullLiteral(),
      new DecimalLiteral(BigDecimal.ONE),
      new TimeLiteral(new Time(1000L)),
      new DateLiteral(new Date(864000000L)),
      new TimestampLiteral(new Timestamp(0))
  );

  private MetaStore metaStore;

  @Mock
  private BiFunction<Expression, Context<Object>, Optional<Expression>> plugin;
  @Mock
  private BiFunction<Expression, Object, Expression> processor;
  @Mock
  private Expression expr1;
  @Mock
  private Expression expr2;
  @Mock
  private Expression expr3;
  @Mock
  private InListExpression inList;
  @Mock
  private WhenClause when1;
  @Mock
  private WhenClause when2;
  @Mock
  private Type type;
  @Mock
  private Object context;

  private ExpressionTreeRewriter<Object> expressionRewriter;
  private ExpressionTreeRewriter<Object> expressionRewriterWithPlugin;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    expressionRewriter = new ExpressionTreeRewriter<>((e, c) -> Optional.empty(), processor);
    expressionRewriterWithPlugin = new ExpressionTreeRewriter<>(plugin, processor);
  }

  @SuppressWarnings("unchecked")
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  private void shouldRewriteUsingPlugin(final Expression parsed) {
    // Given:
    when(plugin.apply(any(), any())).thenReturn(Optional.of(expr1));

    // When:
    final Expression rewritten = expressionRewriterWithPlugin.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, is(expr1));
    final ArgumentCaptor<Context> captor = ArgumentCaptor.forClass(Context.class);
    verify(plugin).apply(same(parsed), captor.capture());
    assertThat(captor.getValue().getContext(), is(context));
  }

  @Test
  public void shouldRewriteArithmeticBinary() {
    // Given:
    final ArithmeticBinaryExpression parsed = parseExpression("1 + 2");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new ArithmeticBinaryExpression(
                parsed.getLocation(),
                parsed.getOperator(),
                expr1,
                expr2
            )
        )
    );
  }

  @Test
  public void shouldRewriteArithmeticBinaryUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 + 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLambdaFunctionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("TRANSFORM_ARRAY(Array[1,2], X => X + Col0)");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteBetweenPredicate() {
    // Given:
    final BetweenPredicate parsed = parseExpression("1 BETWEEN 0 AND 2");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getMin(), context)).thenReturn(expr2);
    when(processor.apply(parsed.getMax(), context)).thenReturn(expr3);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new BetweenPredicate(parsed.getLocation(), expr1, expr2, expr3))
    );
  }

  @Test
  public void shouldRewriteBetweenPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 BETWEEN 0 AND 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteComparisonExpression() {
    // Given:
    final ComparisonExpression parsed = parseExpression("1 < 2");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new ComparisonExpression(parsed.getLocation(), parsed.getType(), expr1, expr2))
    );
  }

  @Test
  public void shouldRewriteComparisonExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 < 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInListExpression() {
    // Given:
    final InPredicate inPredicate = parseExpression("1 IN (1, 2, 3)");
    final InListExpression parsed = inPredicate.getValueList();
    when(processor.apply(parsed.getValues().get(0), context)).thenReturn(expr1);
    when(processor.apply(parsed.getValues().get(1), context)).thenReturn(expr2);
    when(processor.apply(parsed.getValues().get(2), context)).thenReturn(expr3);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new InListExpression(parsed.getLocation(), ImmutableList.of(expr1, expr2, expr3)))
    );
  }

  @Test
  public void shouldRewriteInListUsingPlugin() {
    // Given:
    final InPredicate inPredicate = parseExpression("1 IN (1, 2, 3)");
    final InListExpression parsed = inPredicate.getValueList();

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteFunctionCall() {
    // Given:
    final FunctionCall parsed = parseExpression("STRLEN('foo')");
    when(processor.apply(parsed.getArguments().get(0), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new FunctionCall(parsed.getLocation(), parsed.getName(), ImmutableList.of(expr1)))
    );
  }

  @Test
  public void shouldRewriteFunctionCallUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("STRLEN('foo')");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSimpleCaseExpression() {
    // Given:
    final SimpleCaseExpression parsed = parseExpression(
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END");
    when(processor.apply(parsed.getOperand(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getWhenClauses().get(0), context)).thenReturn(when1);
    when(processor.apply(parsed.getWhenClauses().get(1), context)).thenReturn(when2);
    when(processor.apply(parsed.getDefaultValue().get(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new SimpleCaseExpression(
                parsed.getLocation(),
                expr1,
                ImmutableList.of(when1, when2),
                Optional.of(expr2)
            )
        )
    );
  }

  @Test
  public void shouldRewriteSimpleCaseExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression(
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END"
    );

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInPredicate() {
    // Given:
    final InPredicate parsed = parseExpression("1 IN (1, 2, 3)");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getValueList(), context)).thenReturn(inList);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new InPredicate(parsed.getLocation(), expr1, inList)));
  }

  @Test
  public void shouldRewriteInPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 IN (1, 2, 3)");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteDereferenceExpression() {
    // Given:
    final DereferenceExpression parsed = parseExpression("col0->foo");
    when(processor.apply(parsed.getBase(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new DereferenceExpression(parsed.getLocation(), expr1, parsed.getFieldName()))
    );
  }

  @Test
  public void shouldRewriteDereferenceExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0->foo");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteArithmeticUnary() {
    // Given:
    final ArithmeticUnaryExpression parsed = parseExpression("-(1)");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new ArithmeticUnaryExpression(parsed.getLocation(), parsed.getSign(), expr1))
    );
  }

  @Test
  public void shouldRewriteArithmeticUnaryUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("-1");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteNotExpression() {
    // Given:
    final NotExpression parsed = parseExpression("NOT 1 < 2");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new NotExpression(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteNotExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("NOT 1 < 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSearchedCaseExpression() {
    // Given:
    final SearchedCaseExpression parsed = parseExpression(
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END");
    when(processor.apply(parsed.getWhenClauses().get(0), context)).thenReturn(when1);
    when(processor.apply(parsed.getWhenClauses().get(1), context)).thenReturn(when2);
    when(processor.apply(any(StringLiteral.class), any())).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new SearchedCaseExpression(
                parsed.getLocation(),
                ImmutableList.of(when1, when2),
                Optional.of(expr1)
            )
        )
    );
  }

  @Test
  public void shouldRewriteSearchedCaseExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression(
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLikePredicate() {
    // Given:
    final LikePredicate parsed = parseExpression("col1 LIKE '%foo%' ESCAPE '!'");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getPattern(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new LikePredicate(parsed.getLocation(), expr1, expr2, Optional.of('!'))));
  }

  @Test
  public void shouldRewriteLikePredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col1 LIKE '%foo%'");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNotNullPredicate() {
    // Given:
    final IsNotNullPredicate parsed = parseExpression("col0 IS NOT NULL");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNotNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNotNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NOT NULL");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNullPredicate() {
    // Given:
    final IsNullPredicate parsed = parseExpression("col0 IS NULL");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NULL");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSubscriptExpression() {
    // Given:
    final SubscriptExpression parsed = parseExpression("col4[1]");
    when(processor.apply(parsed.getBase(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getIndex(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new SubscriptExpression(parsed.getLocation(), expr1, expr2)));
  }

  @Test
  public void shouldRewriteCreateArrayExpression() {
    // Given:
    final CreateArrayExpression parsed = parseExpression("ARRAY['foo', col4[1]]");
    final Expression firstVal = parsed.getValues().get(0);
    final Expression secondVal = parsed.getValues().get(1);
    when(processor.apply(firstVal, context)).thenReturn(expr1);
    when(processor.apply(secondVal, context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new CreateArrayExpression(ImmutableList.of(expr1, expr2))));
  }

  @Test
  public void shouldRewriteCreateMapExpression() {
    // Given:
    final CreateMapExpression parsed = parseExpression("MAP('foo' := SUBSTRING('foo',0), 'bar' := col4[1])");
    final Expression firstVal = parsed.getMap().get(new StringLiteral("foo"));
    final Expression secondVal = parsed.getMap().get(new StringLiteral("bar"));
    when(processor.apply(firstVal, context)).thenReturn(expr1);
    when(processor.apply(secondVal, context)).thenReturn(expr2);
    when(processor.apply(new StringLiteral("foo"), context)).thenReturn(new StringLiteral("foo"));
    when(processor.apply(new StringLiteral("bar"), context)).thenReturn(new StringLiteral("bar"));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten,
        equalTo(new CreateMapExpression(
            ImmutableMap.of(new StringLiteral("foo"), expr1, new StringLiteral("bar"), expr2))));
  }

  @Test
  public void shouldRewriteStructExpression() {
    // Given:
    final CreateStructExpression parsed = parseExpression("STRUCT(FOO := 'foo', BAR := col4[1])");
    final Expression fooVal = parsed.getFields().stream().filter(f -> f.getName().equals("FOO")).findFirst().get().getValue();
    final Expression barVal = parsed.getFields().stream().filter(f -> f.getName().equals("BAR")).findFirst().get().getValue();
    when(processor.apply(fooVal, context)).thenReturn(expr1);
    when(processor.apply(barVal, context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new CreateStructExpression(ImmutableList.of(new Field("FOO", expr1), new Field("BAR", expr2)))));
  }

  @Test
  public void shouldRewriteSubscriptExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col4[1]");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLogicalBinaryExpression() {
    // Given:
    final LogicalBinaryExpression parsed = parseExpression("true OR false");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new LogicalBinaryExpression(parsed.getLocation(), parsed.getType(), expr1, expr2))
    );
  }

  @Test
  public void shouldRewriteLogicalBinaryExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("true OR false");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteCast() {
    // Given:
    final Cast parsed = parseExpression("CAST(col0 AS INTEGER)");
    when(processor.apply(parsed.getType(), context)).thenReturn(type);
    when(processor.apply(parsed.getExpression(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new Cast(parsed.getLocation(), expr1, type)));
  }

  @Test
  public void shouldRewriteCastUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("CAST(col0 AS INTEGER)");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteQualifiedColumnReference() {
    // Given:
    final QualifiedColumnReferenceExp expression = new QualifiedColumnReferenceExp(
        SourceName.of("bar"),
        ColumnName.of("foo")
    );

    // When:
    final Expression rewritten = expressionRewriter.rewrite(expression, context);

    // Then:
    assertThat(rewritten, is(expression));
  }

  @Test
  public void shouldRewriteQualifiedColumnReferenceUsingPlugin() {
    // Given:
    final QualifiedColumnReferenceExp expression = new QualifiedColumnReferenceExp(
        SourceName.of("bar"),
        ColumnName.of("foo")
    );

    // When/Then:
    shouldRewriteUsingPlugin(expression);
  }

  @Test
  public void shouldRewriteColumnReference() {
    // Given:
    final UnqualifiedColumnReferenceExp expression = new UnqualifiedColumnReferenceExp(
        ColumnName.of("foo"));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(expression, context);

    // Then:
    assertThat(rewritten, is(expression));
  }

  @Test
  public void shouldRewriteColumnReferenceUsingPlugin() {
    // Given:
    final UnqualifiedColumnReferenceExp expression = new UnqualifiedColumnReferenceExp(
        ColumnName.of("foo"));

    // When/Then:
    shouldRewriteUsingPlugin(expression);
  }

  @Test
  public void shouldRewriteLiteral() {
    for (final Expression expression : LITERALS) {
      // When:
      final Expression rewritten = expressionRewriter.rewrite(expression, context);

      // Then:
      assertThat(rewritten, is(expression));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRewriteLiteralUsingPlugin() {
    for (final Expression expression : LITERALS) {
      reset(plugin);
      shouldRewriteUsingPlugin(expression);
    }
  }

  @Test
  public void shouldRewriteType() {
    // Given:
    final Type type = new Type(SqlPrimitiveType.of("INTEGER"));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(type, context);

    // Then:
    assertThat(rewritten, is(type));
  }

  @Test
  public void shouldRewriteTypeUsingPlugin() {
    final Type type = new Type(SqlPrimitiveType.of("INTEGER"));
    shouldRewriteUsingPlugin(type);
  }

  @Test
  public void shouldRewriteIntervalUnitUsingPlugin() {
    // Given:
    final IntervalUnit expression = new IntervalUnit(TimeUnit.DAYS);
    shouldRewriteUsingPlugin(expression);
  }

  @SuppressWarnings("unchecked")
  private <T extends Expression> T parseExpression(final String asText) {
    final String ksql = String.format("SELECT %s FROM test1;", asText);

    final PreparedStatement<Query> stmt = KsqlParserTestUtil.buildSingleAst(ksql, metaStore);
    final SelectItem selectItem = stmt.getStatement().getSelect().getSelectItems().get(0);
    return (T) ((SingleColumn) selectItem).getExpression();
  }
}
