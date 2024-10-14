package io.confluent.ksql.execution.codegen;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class CompiledExpressionTest {

  private static final Long RETURN_VALUE = 12345L;
  private static final SqlType EXPRESSION_TYPE = SqlTypes.BIGINT;
  private static final Object DEFAULT_VAL = new Object();

  @Mock
  private IExpressionEvaluator expressionEvaluator;
  @Mock
  private Kudf udf;
  @Mock
  private Expression expression;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private Supplier<String> errorMsgSupplier;
  private CompiledExpression compiledExpression;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private CodeGenSpec.Builder spec;

  @Before
  public void setup() throws Exception {
    when(expressionEvaluator.evaluate(any())).thenReturn(RETURN_VALUE);
    when(errorMsgSupplier.get()).thenReturn("It went wrong!");

    spec = new CodeGenSpec.Builder();
  }

  @Test
  public void shouldEvaluateExpressionWithValueColumnSpecs() throws Exception {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );
    spec.addParameter(
        ColumnName.of("foo2"),
        Integer.class,
        1
    );
    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    final Object result = compiledExpression
        .evaluate(genericRow(123, 456), DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(expressionEvaluator).evaluate(new Object[]{123, 456, DEFAULT_VAL, processingLogger, genericRow(123, 456)});
  }

  @Test
  public void shouldEvaluateExpressionWithUdfsSpecs() throws Exception {
    // Given:
    spec.addFunction(
        FunctionName.of("foo"),
        udf
    );
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    final Object result = compiledExpression
        .evaluate(genericRow(123), DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(expressionEvaluator).evaluate(new Object[]{udf, 123, DEFAULT_VAL, processingLogger, genericRow(123)});
  }

  @Test
  public void shouldPerformThreadSafeParameterEvaluation() throws Exception {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );
    spec.addParameter(
        ColumnName.of("foo2"),
        Integer.class,
        1
    );

    final CountDownLatch threadLatch = new CountDownLatch(1);
    final CountDownLatch mainLatch = new CountDownLatch(1);

    when(expressionEvaluator.evaluate(new Object[]{123, 456, DEFAULT_VAL, processingLogger, genericRow(123, 456)}))
        .thenAnswer(
            invocation -> {
              threadLatch.countDown();
              assertThat(mainLatch.await(10, TimeUnit.SECONDS), is(true));
              return RETURN_VALUE;
            });

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    final Thread thread = new Thread(
        () -> compiledExpression
            .evaluate(genericRow(123, 456), DEFAULT_VAL, processingLogger, errorMsgSupplier)
    );

    // When:
    thread.start();

    // Then:
    assertThat(threadLatch.await(10, TimeUnit.SECONDS), is(true));

    // When:
    compiledExpression
        .evaluate(genericRow(100, 200), DEFAULT_VAL, processingLogger, errorMsgSupplier);
    mainLatch.countDown();

    // Then:
    thread.join();
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{123, 456, DEFAULT_VAL, processingLogger, genericRow(123, 456)});
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{100, 200, DEFAULT_VAL, processingLogger, genericRow(100, 200)});
  }

  @Test
  public void shouldLogIfEvalThrows() throws Exception {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    final RuntimeException e = new RuntimeException("Boom");
    when(expressionEvaluator.evaluate(any())).thenThrow(new InvocationTargetException(e));

    final GenericRow row = genericRow(123);

    // When:
    compiledExpression
        .evaluate(row, DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    verify(processingLogger).error(RecordProcessingError
        .recordProcessingError("It went wrong!", e, row));
  }

  @Test
  public void shouldReturnDefaultIfEvalThrows() throws Exception {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    final RuntimeException e = new RuntimeException("Boom");
    when(expressionEvaluator.evaluate(any())).thenThrow(new InvocationTargetException(e));

    // When:
    final Object result = compiledExpression
        .evaluate(genericRow(123), DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    assertThat(result, is(DEFAULT_VAL));
  }

  @Test
  public void shouldReturnDefaultIfThrowsGettingParams() {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    final Object result = compiledExpression
        .evaluate(null, DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    assertThat(result, is(DEFAULT_VAL));
  }

  @Test
  public void shouldLogIfGettingParamsThrows() throws Exception {
    // Given:
    spec.addParameter(
        ColumnName.of("foo1"),
        Integer.class,
        0
    );

    compiledExpression = new CompiledExpression(
        expressionEvaluator,
        spec.build(),
        EXPRESSION_TYPE,
        expression
    );

    // When:
    compiledExpression
        .evaluate(null, DEFAULT_VAL, processingLogger, errorMsgSupplier);

    // Then:
    ArgumentCaptor<RecordProcessingError> err = ArgumentCaptor.forClass(
        RecordProcessingError.class);
    verify(processingLogger).error(err.capture());

    assertThat(err.getValue().getException().get(), instanceOf(NullPointerException.class));
    assertThat(err.getValue().getMessage(), containsString("It went wrong!"));
  }
}