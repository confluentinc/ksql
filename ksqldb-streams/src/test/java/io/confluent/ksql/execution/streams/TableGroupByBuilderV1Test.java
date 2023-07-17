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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
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
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableGroupByV1;
import io.confluent.ksql.execution.streams.TableGroupByBuilderBase.ParamsFactory;
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
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
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

public class TableGroupByBuilderV1Test {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("PAC"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("MAN"), SqlTypes.STRING)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final LogicalSchema REKEYED_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumns(SCHEMA.value())
      .build();

  private static final PhysicalSchema REKEYED_PHYSICAL_SCHEMA =
      PhysicalSchema.from(REKEYED_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final List<Expression> GROUPBY_EXPRESSIONS = ImmutableList.of(
      columnReference("PAC"),
      columnReference("MAN")
  );

  private static final QueryContext STEP_CONTEXT =
      new QueryContext.Stacker().push("foo").push("groupby").getQueryContext();

  private static final ExecutionStepPropertiesV1 PROPERTIES = new ExecutionStepPropertiesV1(
      STEP_CONTEXT
  );

  private static final Formats FORMATS = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.JSON.name()),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  private static final GenericKey KEY = GenericKey.genericKey("key");

  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private GroupedFactory groupedFactory;
  @Mock
  private ExecutionStep<KTableHolder<GenericKey>> sourceStep;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Grouped<GenericKey, GenericRow> grouped;
  @Mock
  private KTable<GenericKey, GenericRow> sourceTable;
  @Mock
  private KTable<GenericKey, GenericRow> filteredTable;
  @Mock
  private KGroupedTable<GenericKey, GenericRow> groupedTable;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ParamsFactory paramsFactory;
  @Mock
  private KTableHolder<GenericKey> tableHolder;
  @Mock
  private GroupByParams groupByParams;
  @Mock
  private Function<GenericRow, GenericKey> mapper;
  @Captor
  private ArgumentCaptor<Predicate<GenericKey, GenericRow>> predicateCaptor;

  private TableGroupByV1<GenericKey> groupBy;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();
  private TableGroupByBuilderV1 builder;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(tableHolder.getSchema()).thenReturn(SCHEMA);
    when(tableHolder.getTable()).thenReturn(sourceTable);

    when(paramsFactory.build(any(), any(), any())).thenReturn(groupByParams);

    when(groupByParams.getSchema()).thenReturn(REKEYED_SCHEMA);
    when(groupByParams.getMapper()).thenReturn(mapper);

    when(buildContext.getKsqlConfig()).thenReturn(ksqlConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(groupedFactory.create(any(), any(Serde.class), any())).thenReturn(grouped);
    when(sourceTable.filter(any())).thenReturn(filteredTable);
    when(filteredTable.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedTable);

    groupBy = new TableGroupByV1<>(
        PROPERTIES,
        sourceStep,
        FORMATS,
        GROUPBY_EXPRESSIONS
    );

    builder = new TableGroupByBuilderV1(buildContext, groupedFactory, paramsFactory);
  }

  @Test
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedTableHolder result = build(builder, tableHolder, groupBy);

    // Then:
    assertThat(result.getGroupedTable(), is(groupedTable));
    verify(sourceTable).filter(any());
    verify(filteredTable).groupBy(any(), same(grouped));
    verifyNoMoreInteractions(filteredTable, sourceTable);
  }

  @Test
  public void shouldBuildGroupByParamsCorrectly() {
    // When:
    build(builder, tableHolder, groupBy);

    // Then:
    verify(paramsFactory).build(
        eq(SCHEMA),
        any(),
        eq(processingLogger)
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KGroupedTableHolder result = build(builder, tableHolder, groupBy);

    // Then:
    assertThat(result.getSchema(), is(REKEYED_SCHEMA));
  }

  @Test
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    build(builder, tableHolder, groupBy);

    // Then:
    verify(sourceTable).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = predicateCaptor.getValue();
    assertThat(predicate.test(KEY, new GenericRow()), is(true));
    assertThat(predicate.test(KEY, null), is(false));
  }

  @Test
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    build(builder, tableHolder, groupBy);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    build(builder, tableHolder, groupBy);

    // Then:
    verify(buildContext).buildKeySerde(
        FORMATS.getKeyFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CONTEXT
    );
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    build(builder, tableHolder, groupBy);

    // Then:
    verify(buildContext).buildValueSerde(
        FORMATS.getValueFormat(),
        REKEYED_PHYSICAL_SCHEMA,
        STEP_CONTEXT
    );
  }

  private static <K> KGroupedTableHolder build(
      final TableGroupByBuilderV1 builder,
      final KTableHolder<K> table,
      final TableGroupByV1<K> step
  ) {
    return builder.build(
        table,
        step.getProperties().getQueryContext(),
        step.getInternalFormats(),
        step.getGroupByExpressions()
    );
  }

  private static Expression columnReference(final String column) {
    return new UnqualifiedColumnReferenceExp(ColumnName.of(column));
  }
}