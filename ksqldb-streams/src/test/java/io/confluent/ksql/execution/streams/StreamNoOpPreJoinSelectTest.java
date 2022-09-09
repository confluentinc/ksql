/*
 * Copyright 2022 Confluent Inc.
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamNoOpPreJoinSelect;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
public class StreamNoOpPreJoinSelectTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("foo"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("bar"), SqlTypes.INTEGER)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final Expression EXPRESSION1 = new StringLiteral("baz");
  private static final Expression EXPRESSION2 = new IntegerLiteral(123);

  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
      SelectExpression.of(ColumnName.of("foo"), EXPRESSION1),
      SelectExpression.of(ColumnName.of("bar"), EXPRESSION2)
  );

  @Mock
  private ExecutionStep<KStreamHolder<Struct>> sourceStep;
  @Mock
  private ExecutionStepPropertiesV1 properties;
  @Mock
  private KStream<Struct, GenericRow> sourceKStream;
  @Mock
  private KStreamHolder<Struct> sourceKStreamHolder;
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private PlanInfo planInfo;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final QueryContext context =
      new QueryContext.Stacker().push("foo").push("bar").getQueryContext();

  private PlanBuilder planBuilder;
  private StreamSelect<Struct> step;

  @Before
  public void setup() {
    when(properties.getQueryContext()).thenReturn(context);
    when(buildContext.getFunctionRegistry()).thenReturn(mock(FunctionRegistry.class));
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    final KStreamHolder<Struct> sourceStream
        = new KStreamHolder<>(sourceKStream, SCHEMA, executionKeyFactory);
    when(sourceStep.build(any(), eq(planInfo), eq(true))).thenReturn(sourceStream);
    step = new StreamNoOpPreJoinSelect<>(
        properties,
        sourceStep,
        ImmutableList.of(),
        Optional.empty(),
        SELECT_EXPRESSIONS
    );
    planBuilder = new KSPlanBuilder(
        buildContext,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldReturnSourceKStreamUnmodified() {
    // When:
    final KStreamHolder<Struct> result = step.build(planBuilder, planInfo, true);

    // Then:
    assertThat(result.getStream(), is(sourceKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldNotAddTransform() {
    // When:
    when(sourceStep.build(any(), eq(planInfo), eq(true))).thenReturn(sourceKStreamHolder);
    when(sourceKStreamHolder.getSchema()).thenReturn(SCHEMA);
    when(sourceKStreamHolder.getStream()).thenReturn(sourceKStream);
    step.build(planBuilder, planInfo);

    // Then:
    verify(sourceKStream, never()).transformValues(
        any(ValueTransformerWithKeySupplier.class),
        any(Named.class)
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<Struct> result = step.build(planBuilder, planInfo, true);

    // Then:
    assertThat(
        result.getSchema().withPseudoAndKeyColsInValue(false),
        is(SCHEMA)
    );
  }
}
