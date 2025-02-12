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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class ForeignKeyTableTableJoinBuilderTest {

  private static final ColumnName L_KEY = ColumnName.of("L_KEY");
  private static final ColumnName L_KEY_2 = ColumnName.of("L_KEY_2");
  private static final ColumnName R_KEY = ColumnName.of("R_KEY");
  private static final ColumnName JOIN_COLUMN = ColumnName.of("L_FOREIGN_KEY");

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .keyColumn(L_KEY, SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
      .valueColumn(JOIN_COLUMN, SqlTypes.STRING)
      .valueColumn(L_KEY, SqlTypes.STRING) // Copy of key in value
      .build();

  private static final String LEFT_KEY = "leftKey";
  private static final String FOREIGN_KEY = "foreignKey";
  private static final GenericRow LEFT_ROW = GenericRow.genericRow(
      1,
      FOREIGN_KEY,
      LEFT_KEY
  );

  private static final LogicalSchema LEFT_SCHEMA_MULTI_KEY = LogicalSchema.builder()
      .keyColumn(L_KEY, SqlTypes.STRING)
      .keyColumn(L_KEY_2, SqlTypes.STRING)
      .valueColumn(JOIN_COLUMN, SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
      .valueColumn(L_KEY, SqlTypes.STRING) // Copy of key in value
      .valueColumn(L_KEY_2, SqlTypes.STRING) // Copy of key in value
      .build();

  private static final String LEFT_KEY_2 = "leftKey2";
  private static final GenericRow LEFT_ROW_MULTI = GenericRow.genericRow(
      FOREIGN_KEY,
      1,
      LEFT_KEY,
      LEFT_KEY_2
  );

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .keyColumn(R_KEY, SqlTypes.STRING)
      .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
      .valueColumn(R_KEY, SqlTypes.STRING) // Copy of key in value
      .build();

  @Mock
  private QueryContext ctx;
  @Mock
  private KTable<Struct, GenericRow> leftKTable;
  @Mock
  private KTable<Struct, GenericRow> leftKTableMultiKey;
  @Mock
  private KTable<Struct, GenericRow> rightKTable;
  @Mock
  private KTable<Struct, GenericRow> resultKTable;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> leftMultiKey;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;
  @Mock
  private Formats formats;

  private PlanBuilder planBuilder;
  private ForeignKeyTableTableJoin<Struct, Struct> join;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(left.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(leftKTable, LEFT_SCHEMA, executionKeyFactory, materializationBuilder));
    when(leftMultiKey.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(leftKTableMultiKey, LEFT_SCHEMA_MULTI_KEY, executionKeyFactory, materializationBuilder));
    when(right.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(rightKTable, RIGHT_SCHEMA, executionKeyFactory, materializationBuilder));

    when(leftKTable.leftJoin(any(KTable.class), any(KsqlKeyExtractor.class), any(), any(Materialized.class))).thenReturn(resultKTable);
    when(leftKTable.join(any(KTable.class), any(KsqlKeyExtractor.class), any(), any(Materialized.class))).thenReturn(resultKTable);
    when(leftKTableMultiKey.leftJoin(any(KTable.class), any(KsqlKeyExtractor.class), any(), any(Materialized.class))).thenReturn(resultKTable);
    when(leftKTableMultiKey.join(any(KTable.class), any(KsqlKeyExtractor.class), any(), any(Materialized.class))).thenReturn(resultKTable);

    when(formats.getKeyFeatures()).thenReturn(mock(SerdeFeatures.class));
    when(formats.getValueFeatures()).thenReturn(mock(SerdeFeatures.class));

    final RuntimeBuildContext context = mock(RuntimeBuildContext.class);
    when((context.getFunctionRegistry())).thenReturn(mock(FunctionRegistry.class));
    when((context.getKsqlConfig())).thenReturn(mock(KsqlConfig.class));
    final MaterializedFactory materializedFactory = mock(MaterializedFactory.class);
    when(materializedFactory.create(any(), any())).thenReturn(mock(Materialized.class));
    when((context.getMaterializedFactory())).thenReturn(materializedFactory);

    planBuilder = new KSPlanBuilder(
        context,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoLeftJoinOnNonKey() {
    // Given:
    givenLeftJoin(left, JOIN_COLUMN);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTable).leftJoin(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(FOREIGN_KEY);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  // this is actually a PK-PK join and the logical planner would not compile a FK-join plan
  // for this case atm
  // however, from a physical plan POV this should still work, so we would like to keep this test
  //
  // it might be possible to actually change the logical planner to compile a PK-PK join as
  // FK-join if input tables are not co-partitioned (instead of throwing an error an rejecting
  // the query), ie, if key-format or partition-count do not match -- it's an open question
  // if it would be a good idea to do this though
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoLeftJoinOnKey() {
    // Given:
    givenLeftJoin(left, L_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTable).leftJoin(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(LEFT_KEY);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoLeftJoinOnSubKey() {
    // Given:
    givenLeftJoin(leftMultiKey, L_KEY_2);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTableMultiKey).leftJoin(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA_MULTI_KEY.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(LEFT_KEY_2);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW_MULTI), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoInnerJoinOnNonKey() {
    // Given:
    givenInnerJoin(left, JOIN_COLUMN);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTable).join(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(FOREIGN_KEY);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  // this is actually a PK-PK join and the logical planner would not compile a FK-join plan
  // for this case atm
  // however, from a physical plan POV this should still work, so we would like to keep this test
  //
  // it might be possible to actually change the logical planner to compile a PK-PK join as
  // FK-join if input tables are not co-partitioned (instead of throwing an error an rejecting
  // the query), ie, if key-format or partition-count do not match -- it's an open question
  // if it would be a good idea to do this though
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoInnerJoinOnKey() {
    // Given:
    givenInnerJoin(left, L_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTable).join(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(LEFT_KEY);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldDoInnerJoinOnSubKey() {
    // Given:
    givenInnerJoin(leftMultiKey, L_KEY_2);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    final ArgumentCaptor<KsqlKeyExtractor> ksqlKeyExtractor
        = ArgumentCaptor.forClass(KsqlKeyExtractor.class);
    verify(leftKTableMultiKey).join(
        same(rightKTable),
        ksqlKeyExtractor.capture(),
        eq(new KsqlValueJoiner(LEFT_SCHEMA_MULTI_KEY.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        any(Materialized.class)
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    final GenericKey extractedKey = GenericKey.genericKey(LEFT_KEY_2);
    assertThat(ksqlKeyExtractor.getValue().apply(LEFT_ROW_MULTI), is(extractedKey));
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // Given:
    givenInnerJoin(left, JOIN_COLUMN);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(LogicalSchema.builder()
            .keyColumns(LEFT_SCHEMA.key())
            .valueColumns(LEFT_SCHEMA.value())
            .valueColumns(RIGHT_SCHEMA.value())
            .build()
        )
    );
  }

  @Test
  public void shouldReturnCorrectSchemaMultiKey() {
    // Given:
    givenInnerJoin(leftMultiKey, L_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(LogicalSchema.builder()
            .keyColumns(LEFT_SCHEMA_MULTI_KEY.key())
            .valueColumns(LEFT_SCHEMA_MULTI_KEY.value())
            .valueColumns(RIGHT_SCHEMA.value())
            .build())
    );
  }

  private void givenLeftJoin(final ExecutionStep<KTableHolder<Struct>> left,
                             final ColumnName leftJoinColumnName) {
    join = new ForeignKeyTableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.LEFT,
        Optional.of(leftJoinColumnName),
        Optional.empty(),
        formats,
        left,
        right
    );
  }

  private void givenInnerJoin(final ExecutionStep<KTableHolder<Struct>> left,
                              final ColumnName leftJoinColumnName) {
    join = new ForeignKeyTableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.INNER,
        Optional.of(leftJoinColumnName),
        Optional.empty(),
        formats,
        left,
        right
    );
  }
}
