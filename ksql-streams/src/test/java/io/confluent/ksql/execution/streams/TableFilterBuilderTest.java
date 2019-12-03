 package io.confluent.ksql.execution.streams;

 import static org.hamcrest.Matchers.is;
 import static org.junit.Assert.assertThat;
 import static org.mockito.ArgumentMatchers.any;
 import static org.mockito.ArgumentMatchers.eq;
 import static org.mockito.Mockito.mock;
 import static org.mockito.Mockito.verify;
 import static org.mockito.Mockito.when;

 import io.confluent.ksql.GenericRow;
 import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
 import io.confluent.ksql.execution.context.QueryContext;
 import io.confluent.ksql.execution.expression.tree.Expression;
 import io.confluent.ksql.execution.materialization.MaterializationInfo;
 import io.confluent.ksql.execution.materialization.MaterializationInfo.TransformFactory;
 import io.confluent.ksql.execution.plan.ExecutionStep;
 import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
 import io.confluent.ksql.execution.plan.KTableHolder;
 import io.confluent.ksql.execution.plan.KeySerdeFactory;
 import io.confluent.ksql.execution.plan.PlanBuilder;
 import io.confluent.ksql.execution.plan.TableFilter;
 import io.confluent.ksql.execution.transform.KsqlProcessingContext;
 import io.confluent.ksql.execution.transform.KsqlTransformer;
 import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
 import io.confluent.ksql.function.FunctionRegistry;
 import io.confluent.ksql.logging.processing.ProcessingLogContext;
 import io.confluent.ksql.logging.processing.ProcessingLogger;
 import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
 import io.confluent.ksql.query.QueryId;
 import io.confluent.ksql.schema.ksql.LogicalSchema;
 import io.confluent.ksql.util.KsqlConfig;
 import java.util.Optional;
 import org.apache.kafka.connect.data.Struct;
 import org.apache.kafka.streams.kstream.KTable;
 import org.apache.kafka.streams.kstream.Named;
 import org.apache.kafka.streams.kstream.ValueMapper;
 import org.junit.Before;
 import org.junit.Rule;
 import org.junit.Test;
 import org.mockito.ArgumentCaptor;
 import org.mockito.Captor;
 import org.mockito.Mock;
 import org.mockito.junit.MockitoJUnit;
 import org.mockito.junit.MockitoRule;

public class TableFilterBuilderTest {

  @Mock
  private SqlPredicateFactory predicateFactory;
  @Mock
  private SqlPredicate sqlPredicate;
  @Mock
  private KsqlTransformer<Struct, Optional<GenericRow>> preTransformer;
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
  private ExecutionStep<KTableHolder<Struct>> sourceStep;
  @Mock
  private ExecutionStepPropertiesV1 sourceProperties;
  @Mock
  private KTable<Struct, GenericRow> sourceKTable;
  @Mock
  private KTable<Struct, Optional<GenericRow>> preKTable;
  @Mock
  private KTable<Struct, Optional<GenericRow>> filteredKTable;
  @Mock
  private KTable<Struct, GenericRow> postKTable;
  @Mock
  private Expression filterExpression;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;
  @Mock
  private Struct key;
  @Mock
  private GenericRow value;
  @Mock
  private KsqlProcessingContext ctx;
  @Captor
  private ArgumentCaptor<TransformFactory<KsqlTransformer<Object, Optional<GenericRow>>>>
      predicateFactoryCaptor;

  private final QueryContext queryContext = new QueryContext.Stacker()
      .push("bar")
      .getQueryContext();

  private PlanBuilder planBuilder;
  private TableFilter<Struct> step;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("foo"));
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(queryBuilder.buildUniqueNodeName(any())).thenAnswer(inv -> inv.getArgument(0) + "-unique");
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLogger);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceProperties.getSchema()).thenReturn(schema);
    when(sourceKTable.transformValues(any(), any(Named.class))).thenReturn((KTable)preKTable);
    when(preKTable.filter(any(), any(Named.class))).thenReturn((KTable)filteredKTable);
    when(filteredKTable.mapValues(any(ValueMapper.class), any(Named.class))).thenReturn(postKTable);
    when(predicateFactory.create(any(), any(), any(), any())).thenReturn(sqlPredicate);
    when(sqlPredicate.getTransformer(any())).thenReturn((KsqlTransformer) preTransformer);
    when(materializationBuilder.filter(any(), any())).thenReturn(materializationBuilder);
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(
        schema,
        queryContext
    );
    step = new TableFilter<>(properties, sourceStep, filterExpression, "stepName");
    when(sourceStep.build(any())).thenReturn(
        KTableHolder.materialized(sourceKTable, schema, keySerdeFactory, materializationBuilder))
    ;
    when(preTransformer.transform(any(), any(), any())).thenReturn(Optional.empty());
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        predicateFactory,
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldFilterSourceTable() {
    // When:
    final KTableHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(postKTable));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KTableHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(schema));
  }

  @Test
  public void shouldBuildSqlPredicateCorrectly() {
    // When:
    step.build(planBuilder);

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
    step.build(planBuilder);

    // Then:
    verify(processingLoggerFactory).getLogger("foo.bar.stepName");
  }

  @Test
  public void shouldFilterMaterialization() {
    // When:
    step.build(planBuilder);

    // Then:
    verify(materializationBuilder).filter(predicateFactoryCaptor.capture(), eq("stepName"));

    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = predicateFactoryCaptor
        .getValue()
        .apply(processingLogger);

    when(preTransformer.transform(any(), any(), any())).thenReturn(Optional.empty());

    // When:
    Optional<GenericRow> result = predicate.transform(key, value, ctx);

    // Then:
    verify(preTransformer).transform(key, value, ctx);
    assertThat(result, is(Optional.empty()));

    // Given:
    when(preTransformer.transform(any(), any(), any()))
        .thenAnswer(inv -> Optional.of(inv.getArgument(1)));

    // When:
    result = predicate.transform(key, value, ctx);

    // Then:
    assertThat(result, is(Optional.of(value)));
  }
}