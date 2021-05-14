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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.streams.StreamGroupByBuilderBase.ParamsFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamGroupByBuilderTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("PAC"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("MAN"), SqlTypes.STRING)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final LogicalSchema REKEYED_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumns(SCHEMA.value())
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA =
      PhysicalSchema.from(SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final PhysicalSchema REKEYED_PHYSICAL_SCHEMA =
      PhysicalSchema.from(REKEYED_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final List<Expression> GROUP_BY_EXPRESSIONS = ImmutableList.of(
      columnReference("PAC"),
      columnReference("MAN")
  );

  private static final QueryContext STEP_CTX =
      new QueryContext.Stacker().push("foo").push("groupby").getQueryContext();

  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(
      STEP_CTX
  );

  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.JSON.name()),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private GroupedFactory groupedFactory;
  @Mock
  private ExecutionStep<KStreamHolder<GenericKey>> sourceStep;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Grouped<GenericKey, GenericRow> grouped;
  @Mock
  private KStream<GenericKey, GenericRow> sourceStream;
  @Mock
  private KStream<GenericKey, GenericRow> filteredStream;
  @Mock
  private KGroupedStream<GenericKey, GenericRow> groupedStream;
  @Captor
  private ArgumentCaptor<Predicate<GenericKey, GenericRow>> predicateCaptor;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KStreamHolder<GenericKey> streamHolder;
  @Mock
  private ParamsFactory paramsFactory;
  @Mock
  private GroupByParams groupByParams;
  @Mock
  private Function<GenericRow, GenericKey> mapper;

  private StreamGroupBy<GenericKey> groupBy;
  private StreamGroupByKey groupByKey;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private StreamGroupByBuilder builder;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(streamHolder.getSchema()).thenReturn(SCHEMA);
    when(streamHolder.getStream()).thenReturn(sourceStream);

    when(paramsFactory.build(any(), any(), any())).thenReturn(groupByParams);

    when(groupByParams.getSchema()).thenReturn(REKEYED_SCHEMA);
    when(groupByParams.getMapper()).thenReturn(mapper);

    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(groupedFactory.create(any(), any(Serde.class), any())).thenReturn(grouped);
    when(sourceStream.groupByKey(any(Grouped.class))).thenReturn(groupedStream);
    when(sourceStream.filter(any())).thenReturn(filteredStream);
    when(filteredStream.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedStream);

    groupBy = new StreamGroupBy<>(
        PROPERTIES,
        sourceStep,
        FORMATS,
        GROUP_BY_EXPRESSIONS
    );

    groupByKey = new StreamGroupByKey(PROPERTIES, sourceStep, FORMATS);

    builder = new StreamGroupByBuilder(buildContext, groupedFactory, paramsFactory);
  }

  @Test
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedStreamHolder result = buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    assertThat(result.getGroupedStream(), is(groupedStream));
    verify(sourceStream).filter(any());
    verify(filteredStream).groupBy(any(), same(grouped));
    verifyNoMoreInteractions(filteredStream, sourceStream);
  }

  @Test
  public void shouldBuildGroupByParamsCorrectly() {
    // When:
    buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    verify(paramsFactory).build(
        eq(SCHEMA),
        any(),
        eq(processingLogger)
    );
  }

  @Test
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    verify(sourceStream).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = predicateCaptor.getValue();
    assertThat(predicate.test(GenericKey.genericKey("foo"), new GenericRow()), is(true));
    assertThat(predicate.test(GenericKey.genericKey("foo"), null), is(false));
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldReturnCorrectSchemaForGroupBy() {
    // When:
    final KGroupedStreamHolder result = buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    assertThat(result.getSchema(), is(REKEYED_SCHEMA));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    verify(buildContext).buildKeySerde(
        FORMATS.getKeyFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    buildGroupBy(builder, streamHolder, groupBy);

    // Then:
    verify(buildContext).buildValueSerde(
        FORMATS.getValueFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  @Test
  public void shouldReturnCorrectSchemaForGroupByKey() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupByKey);

    // Then:
    assertThat(result.getSchema(), is(SCHEMA));
  }

  @Test
  public void shouldPerformGroupByKeyCorrectly() {
    // When:
    final KGroupedStreamHolder result = builder.build(streamHolder, groupByKey);

    // Then:
    assertThat(result.getGroupedStream(), is(groupedStream));
    verify(sourceStream).groupByKey(grouped);
    verifyNoMoreInteractions(sourceStream);
  }

  @Test
  public void shouldNotBuildGroupByParamsOnGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(paramsFactory, never()).build(any(), any(), any());
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(buildContext).buildKeySerde(
        FORMATS.getKeyFormat(),
        PHYSICAL_SCHEMA,
        STEP_CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupByKey() {
    // When:
    builder.build(streamHolder, groupByKey);

    // Then:
    verify(buildContext).buildValueSerde(
        FORMATS.getValueFormat(),
        PHYSICAL_SCHEMA,
        STEP_CTX
    );
  }

  private static <K> KGroupedStreamHolder buildGroupBy(
      final StreamGroupByBuilder builder,
      final KStreamHolder<K> stream,
      final StreamGroupBy<K> step
  ) {
    return builder.build(
        stream,
        step.getProperties().getQueryContext(),
        step.getInternalFormats(),
        step.getGroupByExpressions()
    );
  }

  private static Expression columnReference(final String column) {
    return new UnqualifiedColumnReferenceExp(ColumnName.of(column));
  }
}