package io.confluent.ksql.execution.streams;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.execution.streams.AggregateParamsFactory.KudafAggregatorFactory;
import io.confluent.ksql.execution.streams.AggregateParamsFactory.KudafUndoAggregatorFactory;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateParamsFactoryTest {

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ARGUMENT0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ARGUMENT1"), SqlTypes.DOUBLE)
      .build();

  private static final List<ColumnName> NON_AGG_COLUMNS = ImmutableList.of(
      INPUT_SCHEMA.value().get(0).name(),
      INPUT_SCHEMA.value().get(2).name()
  );

  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final long INITIAL_VALUE0 = 123;
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT1")))
  );
  private static final FunctionCall TABLE_AGG = new FunctionCall(
      FunctionName.of("TABLE_AGG"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final FunctionCall WINDOW_START = new FunctionCall(
      FunctionName.of("WindowStart"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final String INITIAL_VALUE1 = "initial";
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlAggregateFunction agg0;
  @Mock
  private KsqlAggregateFunction agg1;
  @Mock
  private TableAggregationFunction tableAgg;
  @Mock
  private KsqlAggregateFunction windowStart;
  @Mock
  private KudafAggregatorFactory udafFactory;
  @Mock
  private KudafUndoAggregatorFactory undoUdafFactory;
  @Mock
  private KudafAggregator aggregator;
  @Mock
  private KudafUndoAggregator undoAggregator;
  @Mock
  private KsqlProcessingContext ctx;

  private AggregateParams aggregateParams;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(functionRegistry.getAggregateFunction(same(AGG0.getName()), any(), any()))
        .thenReturn(agg0);
    when(agg0.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE0);
    when(agg0.name()).thenReturn(AGG0.getName());
    when(agg0.returnType()).thenReturn(SqlTypes.INTEGER);
    when(agg0.getAggregateType()).thenReturn(SqlTypes.BIGINT);
    when(functionRegistry.getAggregateFunction(same(AGG1.getName()), any(), any()))
        .thenReturn(agg1);
    when(agg1.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE1);
    when(agg1.name()).thenReturn(AGG1.getName());
    when(agg1.returnType()).thenReturn(SqlTypes.STRING);
    when(agg1.getAggregateType()).thenReturn(SqlTypes.DOUBLE);
    when(functionRegistry.getAggregateFunction(same(TABLE_AGG.getName()), any(), any()))
        .thenReturn(tableAgg);
    when(tableAgg.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE0);
    when(tableAgg.returnType()).thenReturn(SqlTypes.INTEGER);
    when(tableAgg.getAggregateType()).thenReturn(SqlTypes.BIGINT);
    when(tableAgg.name()).thenReturn(TABLE_AGG.getName());
    when(functionRegistry.getAggregateFunction(same(WINDOW_START.getName()), any(), any()))
        .thenReturn(windowStart);
    when(windowStart.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE0);
    when(windowStart.name()).thenReturn(WINDOW_START.getName());
    when(windowStart.returnType()).thenReturn(SqlTypes.BIGINT);
    when(windowStart.getAggregateType()).thenReturn(SqlTypes.BIGINT);

    when(udafFactory.create(anyInt(), any())).thenReturn(aggregator);
    when(undoUdafFactory.create(anyInt(), any())).thenReturn(undoAggregator);

    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).create(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        false
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateAggregatorWithCorrectParams() {
    verify(udafFactory).create(2, ImmutableList.of(agg0, agg1));
  }

  @Test
  public void shouldCreateUndoAggregatorWithCorrectParams() {
    // When:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).createUndoable(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        ImmutableList.of(TABLE_AGG)
    );

    // Then:
    verify(undoUdafFactory).create(2, ImmutableList.of(tableAgg));
  }

  @Test
  public void shouldReturnCorrectAggregator() {
    // When:
    final KudafAggregator aggregator = aggregateParams.getAggregator();

    // Then:
    assertThat(aggregator, is(aggregator));
  }

  @Test
  public void shouldReturnCorrectInitializer() {
    // When:
    final KudafInitializer initializer = aggregateParams.getInitializer();

    // Then:
    assertThat(
        initializer.apply(),
        equalTo(genericRow(null, null, INITIAL_VALUE0, INITIAL_VALUE1))
    );
  }

  @Test
  public void shouldReturnEmptyUndoAggregator() {
    // When:
    final Optional<KudafUndoAggregator> undoAggregator = aggregateParams.getUndoAggregator();

    // Then:
    assertThat(undoAggregator.isPresent(), is(false));
  }

  @Test
  public void shouldReturnUndoAggregator() {
    // Given:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).createUndoable(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        ImmutableList.of(TABLE_AGG)
    );

    // When:
    final KudafUndoAggregator undoAggregator = aggregateParams.getUndoAggregator().get();

    // Then:
    assertThat(undoAggregator, is(undoAggregator));
  }

  @Test
  public void shouldReturnCorrectAggregateSchema() {
    // When:
    final LogicalSchema schema = aggregateParams.getAggregateSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnName.aggregateColumn(0), SqlTypes.BIGINT)
                .valueColumn(ColumnName.aggregateColumn(1), SqlTypes.DOUBLE)
                .build()
        )
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final LogicalSchema schema = aggregateParams.getSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnName.aggregateColumn(0), SqlTypes.INTEGER)
                .valueColumn(ColumnName.aggregateColumn(1), SqlTypes.STRING)
                .build()
        )
    );
  }

  @Test
  public void shouldReturnCorrectWindowedSchema() {
    // Given:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).create(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        true
    );

    // When:
    final LogicalSchema schema = aggregateParams.getSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnName.aggregateColumn(0), SqlTypes.INTEGER)
                .valueColumn(ColumnName.aggregateColumn(1), SqlTypes.STRING)
                .valueColumn(SchemaUtil.WINDOWSTART_NAME, SchemaUtil.WINDOWBOUND_TYPE)
                .valueColumn(SchemaUtil.WINDOWEND_NAME, SchemaUtil.WINDOWBOUND_TYPE)
                .build()
        )
    );
  }
}
