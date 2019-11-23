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
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.NamedTestAccessor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
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

  private static final String SELECT_STEP_NAME = "StepName";

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
  private ArgumentCaptor<ValueTransformerWithKeySupplier<Struct, GenericRow, GenericRow>> mapperCaptor;
  @Captor
  private ArgumentCaptor<Named> nameCaptor;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private final QueryContext context =
      new QueryContext.Stacker().getQueryContext();

  private PlanBuilder planBuilder;
  private StreamMapValues<Struct> step;

  @Before
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
    when(queryBuilder.buildUniqueNodeName(any())).thenAnswer(inv -> inv.getArgument(0) + "-unique");
    when(
        sourceKStream.transformValues(any(ValueTransformerWithKeySupplier.class), any(Named.class)))
        .thenReturn(resultKStream);
    final KStreamHolder<Struct> sourceStream
        = new KStreamHolder<>(sourceKStream, SCHEMA, keySerdeFactory);
    when(sourceStep.build(any())).thenReturn(sourceStream);
    step = new StreamMapValues<>(
        properties,
        sourceStep,
        SELECT_EXPRESSIONS,
        SELECT_STEP_NAME
    );
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldReturnResultKStream() {
    // When:
    final KStreamHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldBuildKsNodeWithRightName() {
    // When:
    step.build(planBuilder);

    // Then:
    verify(sourceKStream).transformValues(
        any(ValueTransformerWithKeySupplier.class),
        nameCaptor.capture()
    );

    assertThat(NamedTestAccessor.getName(nameCaptor.getValue()), is(SELECT_STEP_NAME + "-unique"));
  }

  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(
        result.getSchema(),
        is(LogicalSchema.builder()
            .valueColumn(ColumnName.of("expr1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("expr2"), SqlTypes.INTEGER)
            .build())
    );
  }

  @Test
  public void shouldBuildSelectValueMapperLoggerCorrectly() {
    // When:
    step.build(planBuilder);

    // Then:
    verify(processingLoggerFactory).getLogger("qid.PROJECT");
  }
}
