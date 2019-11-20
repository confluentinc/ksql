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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.AggregateMapInfo;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableAggregateBuilderTest {
  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ARGUMENT0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("ARGUMENT1"), SqlTypes.DOUBLE)
      .build();
  private static final LogicalSchema AGGREGATE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("RESULT0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("RESULT1"), SqlTypes.STRING)
      .build();
  private static final PhysicalSchema PHYSICAL_AGGREGATE_SCHEMA = PhysicalSchema.from(
      AGGREGATE_SCHEMA,
      SerdeOption.none()
  );
  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("ARGUMENT0"))))
  );
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("ARGUMENT1"))))
  );
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);
  private static final QueryContext CTX =
      new QueryContext.Stacker().push("agg").push("regate").getQueryContext();
  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));

  @Mock
  private KGroupedTable<Struct, GenericRow> groupedTable;
  @Mock
  private KTable<Struct, GenericRow> aggregated;
  @Mock
  private KTable<Struct, GenericRow> aggregatedWithResults;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private AggregateParamsFactory aggregateParamsFactory;
  @Mock
  private AggregateParams aggregateParams;
  @Mock
  private KudafInitializer initializer;
  @Mock
  private KudafAggregator aggregator;
  @Mock
  private ValueMapper<GenericRow, GenericRow> resultMapper;
  @Mock
  private KudafUndoAggregator undoAggregator;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized;
  @Mock
  private ExecutionStep<KGroupedTableHolder> sourceStep;

  private PlanBuilder planBuilder;
  private TableAggregate aggregate;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(aggregateParamsFactory.createUndoable(any(), anyInt(), any(), any()))
        .thenReturn(aggregateParams);
    when(aggregateParams.getAggregator()).thenReturn(aggregator);
    when(aggregateParams.getUndoAggregator()).thenReturn(Optional.of(undoAggregator));
    when(aggregateParams.getInitializer()).thenReturn(initializer);
    when(aggregateParams.getAggregateSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregateParams.getSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregator.getResultMapper()).thenReturn(resultMapper);
    when(materializedFactory.<Struct, KeyValueStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(materialized);
    when(groupedTable.aggregate(any(), any(), any(), any(Materialized.class))).thenReturn(
        aggregated);
    when(aggregated.mapValues(any(ValueMapper.class))).thenReturn(aggregatedWithResults);
    aggregate = new TableAggregate(
        new DefaultExecutionStepProperties(AGGREGATE_SCHEMA, CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeOption.none()),
        2,
        FUNCTIONS
    );
    when(sourceStep.build(any())).thenReturn(KGroupedTableHolder.of(groupedTable, INPUT_SCHEMA));
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        aggregateParamsFactory,
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            materializedFactory,
            mock(StreamJoinedFactory.class)
        )
    );
  }

  @Test
  public void shouldBuildAggregateCorrectly() {
    // When:
    final KTableHolder<Struct> result = aggregate.build(planBuilder);

    // Then:
    assertThat(result.getTable(), is(aggregatedWithResults));
    final InOrder inOrder = Mockito.inOrder(groupedTable, aggregated, aggregatedWithResults);
    inOrder.verify(groupedTable).aggregate(initializer, aggregator, undoAggregator, materialized);
    inOrder.verify(aggregated).mapValues(resultMapper);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KTableHolder<Struct> result = aggregate.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(AGGREGATE_SCHEMA));
  }

  @Test
  public void shouldBuildMaterializedWithCorrectSerdesForAggregate() {
    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(materializedFactory).create(same(keySerde), same(valueSerde), any());
  }

  @Test
  public void shouldBuildMaterializedWithCorrectNameForAggregate() {
    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(materializedFactory).create(any(), any(), eq("agg-regate"));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildKeySerde(KEY_FORMAT.getFormatInfo(), PHYSICAL_AGGREGATE_SCHEMA, CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(queryBuilder).buildValueSerde(
        VALUE_FORMAT.getFormatInfo(),
        PHYSICAL_AGGREGATE_SCHEMA,
        CTX
    );
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder);

    // Then:
    verify(aggregateParamsFactory).createUndoable(INPUT_SCHEMA, 2, functionRegistry, FUNCTIONS);
  }

  @Test
  public void shouldBuildMaterializationCorrectlyForAggregate() {
    // When:
    final KTableHolder<?> result = aggregate.build(planBuilder);

    // Then:
    assertThat(result.getMaterializationBuilder().isPresent(), is(true));

    final MaterializationInfo info = result.getMaterializationBuilder().get().build();
    assertThat(info.stateStoreName(), equalTo("agg-regate"));
    assertThat(info.getSchema(), equalTo(AGGREGATE_SCHEMA.withoutMetaColumns()));
    assertThat(info.getStateStoreSchema(), equalTo(AGGREGATE_SCHEMA.withoutMetaColumns()));
    assertThat(info.getTransforms(), hasSize(1));

    final AggregateMapInfo aggMapInfo = (AggregateMapInfo) info.getTransforms().get(0);
    assertThat(aggMapInfo.getInfo().schema(), equalTo(INPUT_SCHEMA));
    assertThat(aggMapInfo.getInfo().aggregateFunctions(), equalTo(FUNCTIONS));
    assertThat(aggMapInfo.getInfo().startingColumnIndex(), equalTo(2));
  }
}