/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.TransformFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
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
  private RuntimeBuildContext buildContext;
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
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;
  @Mock
  private PlanInfo planInfo;
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void init() {
    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceKTable.transformValues(any(), any(Named.class))).thenReturn((KTable)preKTable);
    when(preKTable.filter(any(), any(Named.class))).thenReturn(filteredKTable);
    when(filteredKTable.mapValues(any(ValueMapper.class), any(Named.class))).thenReturn(postKTable);
    when(predicateFactory.create(any(), any(), any(), any())).thenReturn(sqlPredicate);
    when(sqlPredicate.getTransformer(any())).thenReturn((KsqlTransformer) preTransformer);
    when(materializationBuilder.filter(any(), any())).thenReturn(materializationBuilder);
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(queryContext);
    step = new TableFilter<>(properties, sourceStep, filterExpression);
    when(sourceStep.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(sourceKTable, schema, executionKeyFactory, materializationBuilder))
    ;
    when(preTransformer.transform(any(), any())).thenReturn(Optional.empty());
    planBuilder = new KSPlanBuilder(
        buildContext,
        predicateFactory,
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldFilterSourceTable() {
    // When:
    final KTableHolder<Struct> result = step.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getTable(), is(postKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KTableHolder<Struct> result = step.build(planBuilder, planInfo);

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
    verify(buildContext).getProcessingLogger(queryContext);
  }

  @Test
  public void shouldFilterMaterialization() {
    // When:
    step.build(planBuilder, planInfo);

    // Then:
    verify(materializationBuilder).filter(
        predicateFactoryCaptor.capture(),
        eq(queryContext));

    // Given:
    final KsqlTransformer<Object, Optional<GenericRow>> predicate = predicateFactoryCaptor
        .getValue()
        .apply(processingLogger);

    when(preTransformer.transform(any(), any())).thenReturn(Optional.empty());

    // When:
    Optional<GenericRow> result = predicate.transform(key, value);

    // Then:
    verify(preTransformer).transform(key, value);
    assertThat(result, is(Optional.empty()));

    // Given:
    when(preTransformer.transform(any(), any()))
        .thenAnswer(inv -> Optional.of(inv.getArgument(1)));

    // When:
    result = predicate.transform(key, value);

    // Then:
    assertThat(result, is(Optional.of(value)));
  }
}
