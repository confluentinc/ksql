package io.confluent.ksql.execution.codegen;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.lang.reflect.InvocationTargetException;
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

  @Mock
  private IExpressionEvaluator expressionEvaluator;
  @Mock
  private Kudf udf;
  private final SqlType expressionType = SqlTypes.BIGINT;
  @Mock
  private GenericRowValueTypeEnforcer typeEnforcer;
  @Mock
  private Object parameter1;
  @Mock
  private Object parameter2;
  @Mock
  private Expression expression;
  private ExpressionMetadata expressionMetadata;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private CodeGenSpec.Builder spec;

  @Before
  public void setup() throws InvocationTargetException {
    when(typeEnforcer.enforceColumnType(anyInt(), any()))
        .thenReturn(parameter1)
        .thenReturn(parameter2);
    when(expressionEvaluator.evaluate(any())).thenReturn(RETURN_VALUE);
    spec = new CodeGenSpec.Builder();
  }

  @Test
  public void shouldEvaluateExpressionWithNoUdfsCorrectly() throws InvocationTargetException {
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
        expressionType,
        typeEnforcer,
        expression
    );

    // When:
    Object result = expressionMetadata.evaluate(new GenericRow(123, 456));

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(typeEnforcer, times(1)).enforceColumnType(1, 456);
    verify(typeEnforcer, times(1)).enforceColumnType(0, 123);
    verify(expressionEvaluator).evaluate(new Object[]{parameter1, parameter2});
  }

  @Test
  public void shouldEvaluateExpressionWithUdfsCorrectly() throws InvocationTargetException {
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
        expressionType,
        typeEnforcer,
        expression
    );

    // When:
    Object result = expressionMetadata.evaluate(new GenericRow(123));

    // Then:
    assertThat(result, equalTo(RETURN_VALUE));
    verify(typeEnforcer, times(1)).enforceColumnType(0, 123);
    verify(expressionEvaluator).evaluate(new Object[]{udf, parameter1});
  }

  @Test
  public void shouldPerformThreadSafeParameterEvaluation()
      throws InterruptedException, InvocationTargetException {
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
    Object thread1Param1 = 1;
    Object thread1Param2 = 2;
    Object thread2Param1 = 3;
    Object thread2Param2 = 4;
    reset(typeEnforcer);
    when(typeEnforcer.enforceColumnType(0, 123))
        .thenReturn(thread1Param1);
    when(typeEnforcer.enforceColumnType(1, 456))
        .thenAnswer(
            invocation -> {
              threadLatch.countDown();
              assertThat(mainLatch.await(10, TimeUnit.SECONDS), is(true));
              return thread1Param2;
            });
    when(typeEnforcer.enforceColumnType(0, 100))
        .thenReturn(thread2Param1);
    when(typeEnforcer.enforceColumnType(1, 200))
        .thenReturn(thread2Param2);
    expressionMetadata = new ExpressionMetadata(
        expressionEvaluator,
        spec.build(),
        expressionType,
        typeEnforcer,
        expression
    );

    // When:
    Thread thread = new Thread(
        () -> expressionMetadata.evaluate(new GenericRow(123, 456))
    );
    thread.start();
    assertThat(threadLatch.await(10, TimeUnit.SECONDS), is(true));
    expressionMetadata.evaluate(new GenericRow(100, 200));
    mainLatch.countDown();
    thread.join();

    // Then:
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{thread1Param1, thread1Param2});
    verify(expressionEvaluator, times(1))
        .evaluate(new Object[]{thread2Param1, thread2Param2});
  }
}