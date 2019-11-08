package io.confluent.ksql.execution.codegen;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ExpressionMetadataTest {

  private static final Long RETURN_VALUE = 12345L;
  private static final SqlType EXPRESSION_TYPE = SqlTypes.BIGINT;

  @Mock
  private IExpressionEvaluator expressionEvaluator;
  @Mock
  private Kudf udf;
  @Mock
  private Expression expression;
  private ExpressionMetadata expressionMetadata;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private CodeGenSpec.Builder spec;

  @Before
  public void setup() throws Exception {
    when(expressionEvaluator.evaluate(any())).thenReturn(RETURN_VALUE);
    spec = new CodeGenSpec.Builder();
  }

  @Test
  public void shouldEvaluateExpressionWithNoUdfsCorrectly() throws Exception {
    // Given:
    spec.addParameter(
        ColumnRef.withoutSource(ColumnName.of("foo1")),
        Integer.class,
        0
    );
    spec.addParameter(
        ColumnRef.withoutSource(ColumnName.of("foo2")),
        Integer.class,
        1
    );
    expressionMetadata = new ExpressionMetadata(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    Object result = expressionMetadata.evaluate(new GenericRow(123, 456));

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(expressionEvaluator).evaluate(new Object[]{123, 456});
  }

  @Test
  public void shouldEvaluateExpressionWithUdfsCorrectly() throws Exception {
    // Given:
    spec.addFunction(
        FunctionName.of("foo"),
        udf
    );
    spec.addParameter(
        ColumnRef.withoutSource(ColumnName.of("foo1")),
        Integer.class,
        0
    );

    expressionMetadata = new ExpressionMetadata(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    Object result = expressionMetadata.evaluate(new GenericRow(123));

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(expressionEvaluator).evaluate(new Object[]{udf, 123});
  }

  @Test
  public void shouldPerformThreadSafeParameterEvaluation() throws Exception {
    // Given:
    spec.addParameter(
        ColumnRef.withoutSource(ColumnName.of("foo1")),
        Integer.class,
        0
    );
    spec.addParameter(
        ColumnRef.withoutSource(ColumnName.of("foo2")),
        Integer.class,
        1
    );

    CountDownLatch threadLatch = new CountDownLatch(1);
    CountDownLatch mainLatch = new CountDownLatch(1);

    when(expressionEvaluator.evaluate(new Object[]{123, 456}))
        .thenAnswer(
            invocation -> {
              threadLatch.countDown();
              assertThat(mainLatch.await(10, TimeUnit.SECONDS), is(true));
              return RETURN_VALUE;
            });

    expressionMetadata = new ExpressionMetadata(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    Thread thread = new Thread(
        () -> expressionMetadata.evaluate(new GenericRow(123, 456))
    );

    // When:
    thread.start();

    // Then:
    assertThat(threadLatch.await(10, TimeUnit.SECONDS), is(true));

    // When:
    expressionMetadata.evaluate(new GenericRow(100, 200));
    mainLatch.countDown();

    // Then:
    thread.join();
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{123, 456});
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{100, 200});
  }
}