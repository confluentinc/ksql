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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.MapperInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
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
  private static final List<ColumnName> NON_AGG_COLUMNS = ImmutableList.of(
      INPUT_SCHEMA.value().get(0).name(),
      INPUT_SCHEMA.value().get(1).name()
  );
  private static final PhysicalSchema PHYSICAL_AGGREGATE_SCHEMA = PhysicalSchema.from(
      AGGREGATE_SCHEMA,
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );
  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT1")))
  );
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);
  private static final QueryContext CTX =
      new QueryContext.Stacker().push("agg").push("regate").getQueryContext();
  private static final QueryContext MATERIALIZE_CTX = QueryContext.Stacker.of(CTX)
      .push("Materialize").getQueryContext();
  private static final FormatInfo KEY_FORMAT = FormatInfo.of(FormatFactory.KAFKA.name());
  private static final FormatInfo VALUE_FORMAT = FormatInfo.of(FormatFactory.JSON.name());

  @Mock
  private KGroupedTable<GenericKey, GenericRow> groupedTable;
  @Mock
  private KTable<GenericKey, GenericRow> aggregated;
  @Mock
  private KTable<GenericKey, GenericRow> aggregatedWithResults;
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private AggregateParamsFactory aggregateParamsFactory;
  @Mock
  private AggregateParams aggregateParams;
  @Mock
  private KudafInitializer initializer;
  @Mock
  private KudafAggregator<GenericKey> aggregator;
  @Mock
  private KsqlTransformer<GenericKey, GenericRow> resultMapper;
  @Mock
  private KudafUndoAggregator undoAggregator;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized;
  @Mock
  private ExecutionStep<KGroupedTableHolder> sourceStep;
  @Mock
  private KsqlProcessingContext ctx;
  @Mock
  private PlanInfo planInfo;

  private PlanBuilder planBuilder;
  private TableAggregate aggregate;

  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void init() {
    when(buildContext.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.getKsqlConfig()).thenReturn(KsqlConfig.empty());
    when(aggregateParamsFactory.createUndoable(any(), any(), any(), any(), any()))
        .thenReturn(aggregateParams);
    when(aggregateParams.getAggregator()).thenReturn((KudafAggregator)aggregator);
    when(aggregateParams.getUndoAggregator()).thenReturn(Optional.of(undoAggregator));
    when(aggregateParams.getInitializer()).thenReturn(initializer);
    when(aggregateParams.getAggregateSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregateParams.getSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(aggregator.getResultMapper()).thenReturn(resultMapper);
    when(materializedFactory.<GenericKey, KeyValueStore<Bytes, byte[]>>create(any(), any(), any()))
        .thenReturn(materialized);
    when(groupedTable.aggregate(any(), any(), any(), any(Materialized.class))).thenReturn(
        aggregated);
    when(aggregated.transformValues(any(), any(Named.class)))
        .thenReturn((KTable)aggregatedWithResults);
    aggregate = new TableAggregate(
        new ExecutionStepPropertiesV1(CTX),
        sourceStep,
        Formats.of(KEY_FORMAT, VALUE_FORMAT, SerdeFeatures.of(), SerdeFeatures.of()),
        NON_AGG_COLUMNS,
        FUNCTIONS
    );
    when(sourceStep.build(any(), eq(planInfo))).thenReturn(KGroupedTableHolder.of(groupedTable, INPUT_SCHEMA));
    planBuilder = new KSPlanBuilder(
        buildContext,
        mock(SqlPredicateFactory.class),
        aggregateParamsFactory,
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            materializedFactory,
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  @Test
  public void shouldBuildAggregateCorrectly() {
    // When:
    final KTableHolder<GenericKey> result = aggregate.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getTable(), is(aggregatedWithResults));
    final InOrder inOrder = Mockito.inOrder(groupedTable, aggregated, aggregatedWithResults);
    inOrder.verify(groupedTable).aggregate(initializer, aggregator, undoAggregator, materialized);
    inOrder.verify(aggregated).transformValues(any(), any(Named.class));
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KTableHolder<GenericKey> result = aggregate.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getSchema(), is(AGGREGATE_SCHEMA));
  }

  @Test
  public void shouldBuildMaterializedWithCorrectSerdesForAggregate() {
    // When:
    aggregate.build(planBuilder, planInfo);

    // Then:
    verify(materializedFactory).create(same(keySerde), same(valueSerde), any());
  }

  @Test
  public void shouldBuildMaterializedWithCorrectNameForAggregate() {
    // When:
    aggregate.build(planBuilder, planInfo);

    // Then:
    verify(materializedFactory).create(any(), any(), eq("agg-regate-Materialize"));
  }

  @Test
  public void shouldBuildKeySerdeCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildKeySerde(KEY_FORMAT, PHYSICAL_AGGREGATE_SCHEMA, MATERIALIZE_CTX);
  }

  @Test
  public void shouldBuildValueSerdeCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder, planInfo);

    // Then:
    verify(buildContext).buildValueSerde(
        VALUE_FORMAT,
        PHYSICAL_AGGREGATE_SCHEMA,
        MATERIALIZE_CTX
    );
  }

  @Test
  public void shouldBuildAggregatorParamsCorrectlyForAggregate() {
    // When:
    aggregate.build(planBuilder, planInfo);

    // Then:
    verify(aggregateParamsFactory).createUndoable(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        KsqlConfig.empty()
    );
  }

  @Test
  public void shouldBuildMaterializationCorrectlyForAggregate() {
    // When:
    final KTableHolder<?> result = aggregate.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getMaterializationBuilder().isPresent(), is(true));

    final MaterializationInfo info = result.getMaterializationBuilder().get().build();
    assertThat(info.stateStoreName(), equalTo("agg-regate-Materialize"));
    assertThat(info.getSchema(), equalTo(AGGREGATE_SCHEMA));
    assertThat(info.getStateStoreSchema(), equalTo(AGGREGATE_SCHEMA));
    assertThat(info.getTransforms(), hasSize(1));

    final MapperInfo aggMapInfo = (MapperInfo) info.getTransforms().get(0);
    final KsqlTransformer<Object, GenericRow> mapper = aggMapInfo.getMapper(name -> null);

    // Given:
    final GenericKey key = mock(GenericKey.class);
    final GenericRow value = mock(GenericRow.class);

    // When:
    mapper.transform(key, value);

    // Then:
    verify(resultMapper).transform(key, value);
  }
}