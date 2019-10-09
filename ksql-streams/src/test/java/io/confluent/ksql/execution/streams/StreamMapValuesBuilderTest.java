/*
 * Copyright 2019 Confluent Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamMapValuesBuilderTest {
  private static final LogicalSchema SCHEMA = new LogicalSchema.Builder()
      .valueColumn(ColumnName.of("foo"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("bar"), SqlTypes.BIGINT)
      .build()
      .withMetaAndKeyColsInValue()
      .withAlias(SourceName.of("alias"));

  private static final Expression EXPRESSION1 = new StringLiteral("baz");
  private static final Expression EXPRESSION2 = new IntegerLiteral(123);

  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
    SelectExpression.of(ColumnName.of("expr1"), EXPRESSION1),
    SelectExpression.of(ColumnName.of("expr2"), EXPRESSION2)
  );

  @Mock
  private ExecutionStep<KStreamHolder<Struct>> sourceStep;
  @Mock
  private ExecutionStepProperties sourceProperties;
  @Mock
  private ExecutionStepProperties properties;
  @Mock
  private KStream<Struct, GenericRow> sourceKStream;
  @Mock
  private KStream<Struct, GenericRow> resultKStream;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Captor
  private ArgumentCaptor<ValueMapper<GenericRow, GenericRow>> captor;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final QueryContext context =
      new QueryContext.Stacker().getQueryContext();

  private PlanBuilder planBuilder;
  private StreamMapValues<Struct> step;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceProperties.getSchema()).thenReturn(SCHEMA);
    when(properties.getQueryContext()).thenReturn(context);
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(mock(ProcessingLogger.class));
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("qid"));
    when(queryBuilder.getFunctionRegistry()).thenReturn(mock(FunctionRegistry.class));
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(sourceKStream.mapValues(any(ValueMapper.class))).thenReturn(resultKStream);
    final KStreamHolder<Struct> sourceStream
        = new KStreamHolder<>(sourceKStream, keySerdeFactory);
    when(sourceStep.build(any())).thenReturn(sourceStream);
    step = new StreamMapValues<>(
        properties,
        sourceStep,
        SELECT_EXPRESSIONS
    );
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParams.Factory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldCallMapValuesWithMapperWithCorrectExpressions() {
    // When:
    step.build(planBuilder);

    // Then:
    verify(sourceKStream).mapValues(captor.capture());
    assertThat(captor.getValue(), instanceOf(SelectValueMapper.class));
    final SelectValueMapper mapper = (SelectValueMapper) captor.getValue();
    assertThat(mapper.getSelects(), hasSize(2));
    assertThat(mapper.getSelects().get(0).fieldName, equalTo(ColumnName.of("expr1")));
    assertThat(mapper.getSelects().get(0).evaluator.getExpression(), equalTo(EXPRESSION1));
    assertThat(mapper.getSelects().get(1).fieldName, equalTo(ColumnName.of("expr2")));
    assertThat(mapper.getSelects().get(1).evaluator.getExpression(), equalTo(EXPRESSION2));
  }

  @Test
  public void shouldReturnResultKStream() {
    // When:
    final KStreamHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }
}