package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamFilterBuilderTest {
  @Mock
  private SqlPredicateFactory predicateFactory;
  @Mock
  private SqlPredicate sqlPredicate;
  @Mock
  private KsqlTransformer<GenericKey, Optional<GenericRow>> predicate;
  @Mock
  private KsqlQueryBuilder queryBuilder;
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
  private ExecutionStepPropertiesV1 sourceProperties;
  @Mock
  private KStream<GenericKey, GenericRow> sourceKStream;
  @Mock
  private KStream<GenericKey, GenericRow> filteredKStream;
  @Mock
  private Expression filterExpression;
  @Mock
  private ExecutionKeyFactory<GenericKey> executionKeyFactory;
  @Mock
  private PlanInfo planInfo;

  private final QueryContext queryContext = new QueryContext.Stacker()
      .push("bar")
      .getQueryContext();

  private PlanBuilder planBuilder;
  private StreamFilter<GenericKey> step;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("foo"));
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getProcessingLogger(any())).thenReturn(processingLogger);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceKStream
        .flatTransformValues(any(ValueTransformerWithKeySupplier.class), any(Named.class)))
        .thenReturn(filteredKStream);
    when(predicateFactory.create(any(), any(), any(), any())).thenReturn(sqlPredicate);
    when(sqlPredicate.getTransformer(any())).thenReturn((KsqlTransformer) predicate);
    when(sourceStep.build(any(), eq(planInfo))).thenReturn(new KStreamHolder<>(
        sourceKStream,
        schema,
        executionKeyFactory
    ));
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(queryContext);
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        predicateFactory,
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
    step = new StreamFilter<>(properties, sourceStep, filterExpression);
  }

  @Test
  public void shouldFilterSourceStream() {
    // When:
    final KStreamHolder<GenericKey> result = step.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getStream(), is(filteredKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<GenericKey> result = step.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getSchema(), is(schema));
  }

  @Test
  public void shouldBuildSqlPredicateCorrectly() {
    // When:
    step.build(planBuilder, planInfo);

    // Then:
    verify(predicateFactory).create(
        filterExpression,
        schema,
        ksqlConfig,
        functionRegistry
    );
  }

  @Test
  public void shouldUseCorrectNameForProcessingLogger() {
    // When:
    step.build(planBuilder, planInfo);

    // Then:
    verify(queryBuilder).getProcessingLogger(queryContext);
  }
}