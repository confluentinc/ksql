 package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TableFilterBuilderTest {
  @Mock
  private SqlPredicateFactory predicateFactory;
  @Mock
  private SqlPredicate sqlPredicate;
  @Mock
  private Predicate predicate;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private LogicalSchema schema;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private ExecutionStepProperties sourceProperties;
  @Mock
  private KTable<Struct, GenericRow> sourceKTable;
  @Mock
  private KTable<Struct, GenericRow> filteredKTable;
  @Mock
  private Expression filterExpression;

  private final QueryContext queryContext = new QueryContext.Stacker(new QueryId("foo"))
      .push("bar")
      .getQueryContext();

  private TableFilter<KTable<Struct, GenericRow>> step;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLogger);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceProperties.getSchema()).thenReturn(schema);
    when(sourceKTable.filter(any())).thenReturn(filteredKTable);
    when(predicateFactory.create(any(), any(), any(), any(), any())).thenReturn(sqlPredicate);
    when(sqlPredicate.getPredicate()).thenReturn(predicate);
    final ExecutionStepProperties properties = new DefaultExecutionStepProperties(
        schema,
        queryContext
    );
    step = new TableFilter<>(properties, sourceStep, filterExpression);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFilterSourceTable() {
    // When:
    final KTable result = TableFilterBuilder.build(
        sourceKTable,
        step,
        queryBuilder,
        predicateFactory
    );

    // Then:
    assertThat(result, is(filteredKTable));
    verify(sourceKTable).filter(predicate);
  }

  @Test
  public void shouldBuildSqlPredicateCorrectly() {
    // When:
    TableFilterBuilder.build(sourceKTable, step, queryBuilder, predicateFactory);

    // Then:
    verify(predicateFactory).create(
        filterExpression,
        schema,
        ksqlConfig,
        functionRegistry,
        processingLogger
    );
  }

  @Test
  public void shouldUseCorrectNameForProcessingLogger() {
    // When:
    TableFilterBuilder.build(sourceKTable, step, queryBuilder, predicateFactory);

    // Then:
    verify(processingLoggerFactory).getLogger("foo.bar.FILTER");
  }
}