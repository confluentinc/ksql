package io.confluent.ksql.parser.tree;

import static io.confluent.ksql.parser.KsqlParserTestUtil.parseExpression;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter.Context;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ExpressionTreeRewriterTest {
  private static final List<Expression> LITERALS = ImmutableList.of(
      new IntegerLiteral(1),
      new LongLiteral(1),
      new DoubleLiteral(1.0),
      new BooleanLiteral("true"),
      new StringLiteral("abcd"),
      new NullLiteral(),
      new DecimalLiteral("1.0"),
      new TimeLiteral("00:00:00"),
      new TimestampLiteral("00:00:00")
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

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    expressionRewriter = new ExpressionTreeRewriter<>((e, c) -> Optional.empty(), processor);
    expressionRewriterWithPlugin = new ExpressionTreeRewriter<>(plugin, processor);
  }

  @SuppressWarnings("unchecked")
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
    final ArithmeticBinaryExpression parsed
        = (ArithmeticBinaryExpression) parseExpression("1 + 2", metaStore);
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
    final Expression parsed = parseExpression("1 + 2", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteBetweenPredicate() {
    // Given:
    final BetweenPredicate parsed =
        (BetweenPredicate) parseExpression("1 BETWEEN 0 AND 2", metaStore);
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
    final Expression parsed = parseExpression("1 BETWEEN 0 AND 2", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteComparisonExpression() {
    // Given:
    final ComparisonExpression parsed =
        (ComparisonExpression) parseExpression("1 < 2", metaStore);
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
    final Expression parsed = parseExpression("1 < 2", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInListExpression() {
    // Given:
    final InListExpression parsed
        = ((InPredicate) parseExpression("1 IN (1, 2, 3)", metaStore)).getValueList();
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
    final InListExpression parsed
        = ((InPredicate) parseExpression("1 IN (1, 2, 3)", metaStore)).getValueList();

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteFunctionCall() {
    // Given:
    final FunctionCall parsed = (FunctionCall) parseExpression("STRLEN('foo')", metaStore);
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
    final Expression parsed = parseExpression("STRLEN('foo')", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSimpleCaseExpression() {
    // Given:
    final SimpleCaseExpression parsed = (SimpleCaseExpression) parseExpression(
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END",
        metaStore);
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
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END",
        metaStore
    );

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInPredicate() {
    // Given:
    final InPredicate parsed = (InPredicate) parseExpression("1 IN (1, 2, 3)", metaStore);
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
    final Expression parsed = parseExpression("1 IN (1, 2, 3)", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteDereferenceExpression() {
    // Given:
    final DereferenceExpression parsed
        = (DereferenceExpression) parseExpression("col0->foo", metaStore);
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
    final Expression parsed = parseExpression("col0->foo", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteArithmeticUnary() {
    // Given:
    final ArithmeticUnaryExpression parsed
        = (ArithmeticUnaryExpression) parseExpression("-1", metaStore);
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
    final Expression parsed = parseExpression("-1", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteNotExpression() {
    // Given:
    final NotExpression parsed = (NotExpression) parseExpression("NOT 1 < 2", metaStore);
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new NotExpression(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteNotExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("NOT 1 < 2", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSearchedCaseExpression() {
    // Given:
    final SearchedCaseExpression parsed = (SearchedCaseExpression) parseExpression(
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END", metaStore);
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
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLikePredicate() {
    // Given:
    final LikePredicate parsed
        = (LikePredicate) parseExpression("col1 LIKE '%foo%'", metaStore);
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getPattern(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new LikePredicate(parsed.getLocation(), expr1, expr2)));
  }

  @Test
  public void shouldRewriteLikePredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col1 LIKE '%foo%'", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNotNullPredicate() {
    // Given:
    final IsNotNullPredicate parsed
        = (IsNotNullPredicate) parseExpression("col0 IS NOT NULL", metaStore);
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNotNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNotNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NOT NULL", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNullPredicate() {
    // Given:
    final IsNullPredicate parsed
        = (IsNullPredicate) parseExpression("col0 IS NULL", metaStore);
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NULL", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSubscriptExpression() {
    // Given:
    final SubscriptExpression parsed
        = (SubscriptExpression) parseExpression("col4[1]", metaStore);
    when(processor.apply(parsed.getBase(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getIndex(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new SubscriptExpression(parsed.getLocation(), expr1, expr2)));
  }

  @Test
  public void shouldRewriteSubscriptExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col4[1]", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLogicalBinaryExpression() {
    // Given:
    final LogicalBinaryExpression parsed
        = (LogicalBinaryExpression) parseExpression("true OR false", metaStore);
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
    final Expression parsed = parseExpression("true OR false", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteCast() {
    // Given:
    final Cast parsed = (Cast) parseExpression("CAST(col0 AS INTEGER)", metaStore);
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
    final Expression parsed = parseExpression("CAST(col0 AS INTEGER)", metaStore);

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteQualifiedNameReference() {
    // Given:
    final QualifiedNameReference expression = new QualifiedNameReference(QualifiedName.of("foo"));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(expression, context);

    // Then:
    assertThat(rewritten, is(expression));
  }

  @Test
  public void shouldRewriteQualifiedNameReferenceUsingPlugin() {
    // Given:
    final QualifiedNameReference expression = new QualifiedNameReference(QualifiedName.of("foo"));

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
}