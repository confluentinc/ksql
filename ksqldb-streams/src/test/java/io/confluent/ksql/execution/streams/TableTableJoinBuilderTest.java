package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.materialization.MaterializationInfo;
import static io.confluent.ksql.execution.plan.StreamStreamJoin.LEGACY_KEY_COL;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWKEY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableTableJoinBuilderTest {

  private static final ColumnName L_KEY = ColumnName.of("L_KEY");
  private static final ColumnName R_KEY = ColumnName.of("R_KEY");
  private static final ColumnName SYNTH_KEY = ColumnName.of("KSQL_COL_0");

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .keyColumn(L_KEY, SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
      .valueColumn(L_KEY, SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .keyColumn(R_KEY, SqlTypes.STRING)
      .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
      .valueColumn(R_KEY, SqlTypes.STRING)
      .build();

  @Mock
  private QueryContext ctx;
  @Mock
  private KTable<Struct, GenericRow> leftKTable;
  @Mock
  private KTable<Struct, GenericRow> rightKTable;
  @Mock
  private KTable<Struct, GenericRow> resultKTable;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;

  private PlanBuilder planBuilder;
  private TableTableJoin<Struct> join;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(left.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(leftKTable, LEFT_SCHEMA, executionKeyFactory, materializationBuilder));
    when(right.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(rightKTable, RIGHT_SCHEMA, executionKeyFactory, materializationBuilder));

    when(leftKTable.leftJoin(any(KTable.class), any())).thenReturn(resultKTable);
    when(leftKTable.outerJoin(any(KTable.class), any())).thenReturn(resultKTable);
    when(leftKTable.join(any(KTable.class), any())).thenReturn(resultKTable);

    planBuilder = new KSPlanBuilder(
        mock(RuntimeBuildContext.class),
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
  }

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin(L_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKTable).leftJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoLeftJoinWithSyntheticKey() {
    // Given:
    givenLeftJoin(SYNTH_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(leftKTable).leftJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1))
    );
  }

  @Test
  public void shouldDoOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKTable).outerJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin(R_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKTable).join(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoInnerJoinWithSytheticKey() {
    // Given:
    givenInnerJoin(SYNTH_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(leftKTable).join(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1))
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // Given:
    givenInnerJoin(R_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(R_KEY, LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  @Test
  public void shouldReturnCorrectSchemaWithSyntheticKey() {
    // Given:
    givenInnerJoin(SYNTH_KEY);

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(SYNTH_KEY, LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  @Test
  public void shouldReturnCorrectLegacySchema() {
    // Given:
    join = new TableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.INNER,
        ColumnName.of(LEGACY_KEY_COL),
        left,
        right
    );

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(ROWKEY_NAME, LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  private void givenLeftJoin(final ColumnName keyName) {
    join = new TableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.LEFT,
        keyName,
        left,
        right
    );
  }

  private void givenOuterJoin() {
    join = new TableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.OUTER,
        SYNTH_KEY,
        left,
        right
    );
  }

  private void givenInnerJoin(final ColumnName keyName) {
    join = new TableTableJoin<>(
        new ExecutionStepPropertiesV1(ctx),
        JoinType.INNER,
        keyName,
        left,
        right
    );
  }
}
