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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
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
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
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
      .valueField("foo", SqlTypes.STRING)
      .valueField("bar", SqlTypes.BIGINT)
      .build()
      .withMetaAndKeyFieldsInValue()
      .withAlias("alias");

  private static final Expression EXPRESSION1 = new StringLiteral("baz");
  private static final Expression EXPRESSION2 = new IntegerLiteral(123);

  private static final List<SelectExpression> SELECT_EXPRESSIONS = ImmutableList.of(
    SelectExpression.of("expr1", EXPRESSION1),
    SelectExpression.of("expr2", EXPRESSION2)
  );

  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> sourceStep;
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
  @Captor
  private ArgumentCaptor<ValueMapper<GenericRow, GenericRow>> captor;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final QueryContext context =
      new QueryContext.Stacker(new QueryId("qid")).getQueryContext();

  private StreamMapValues<KStream<Struct, GenericRow>> step;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(sourceProperties.getSchema()).thenReturn(SCHEMA);
    when(properties.getQueryContext()).thenReturn(context);
    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(mock(ProcessingLogger.class));
    when(queryBuilder.getFunctionRegistry()).thenReturn(mock(FunctionRegistry.class));
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(sourceKStream.mapValues(any(ValueMapper.class))).thenReturn(resultKStream);
    step = new StreamMapValues<>(
        properties,
        sourceStep,
        SELECT_EXPRESSIONS
    );
  }

  @Test
  public void shouldCallMapValuesWithMapperWithCorrectExpressions() {
    // When:
    StreamMapValuesBuilder.build(
        sourceKStream,
        step,
        queryBuilder
    );

    // Then:
    verify(sourceKStream).mapValues(captor.capture());
    assertThat(captor.getValue(), instanceOf(SelectValueMapper.class));
    final SelectValueMapper mapper = (SelectValueMapper) captor.getValue();
    assertThat(mapper.getSelects(), hasSize(2));
    assertThat(mapper.getSelects().get(0).fieldName, equalTo("expr1"));
    assertThat(mapper.getSelects().get(0).evaluator.getExpression(), equalTo(EXPRESSION1));
    assertThat(mapper.getSelects().get(1).fieldName, equalTo("expr2"));
    assertThat(mapper.getSelects().get(1).evaluator.getExpression(), equalTo(EXPRESSION2));
  }

  @Test
  public void shouldReturnResultKStream() {
    // When:
    final KStream<Struct, GenericRow> kstream = StreamMapValuesBuilder.build(
        sourceKStream,
        step,
        queryBuilder
    );

    // Then:
    assertThat(kstream, is(resultKStream));
  }
}