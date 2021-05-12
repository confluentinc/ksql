package io.confluent.ksql.execution.function.udtf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.FunctionName;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableFunctionApplierTest {

  private static final GenericRow VALUE = GenericRow.genericRow(10L);

  @Mock
  private KsqlTableFunction tableFunction;
  @Mock
  private CompiledExpression paramExtractor;
  @Mock
  private ProcessingLogger processingLogger;
  private TableFunctionApplier applier;

  @Before
  public void setUp() {
    when(tableFunction.name()).thenReturn(FunctionName.of("SOME_FUNC"));

    applier = new TableFunctionApplier(tableFunction, ImmutableList.of(paramExtractor));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCallEvaluatorWithCorrectParams() {
    // When:
    applier.apply(VALUE, processingLogger);

    // Then:
    final ArgumentCaptor<Supplier<String>> errorMsgCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(paramExtractor)
        .evaluate(eq(VALUE), isNull(), eq(processingLogger), errorMsgCaptor.capture());

    assertThat(errorMsgCaptor.getValue().get(),
        is("Failed to evaluate table function parameter 0"));
  }

  @Test
  public void shouldLogProcessingErrorIfUdtfThrows() {
    // Given:
    final RuntimeException e = new RuntimeException("Boom");
    when(tableFunction.apply(any())).thenThrow(e);

    // When:
    applier.apply(VALUE, processingLogger);

    // Then:
    verify(processingLogger).error(RecordProcessingError.recordProcessingError(
        "Table function SOME_FUNC threw an exception",
        e,
        VALUE
    ));
  }

  @Test
  public void shouldReturnEmptyListIfUdtfThrows() {
    // Given:
    final RuntimeException e = new RuntimeException("Boom");
    when(tableFunction.apply(any())).thenThrow(e);

    // When:
    final List<?> result = applier.apply(VALUE, processingLogger);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldLogIfUdtfReturnsNull() {
    // Given:
    when(tableFunction.apply(any())).thenReturn(null);

    // When:
    applier.apply(VALUE, processingLogger);

    // Then:
    verify(processingLogger).error(RecordProcessingError.recordProcessingError(
        "Table function SOME_FUNC returned null. This is invalid. "
            + "Table functions should always return a valid list.",
        VALUE
    ));
  }

  @Test
  public void shouldReturnEmptyListIfUdtfReturnsNull() {
    // Given:
    when(tableFunction.apply(any())).thenReturn(null);

    // When:
    final List<?> result = applier.apply(VALUE, processingLogger);

    // Then:
    assertThat(result, is(empty()));
  }
}