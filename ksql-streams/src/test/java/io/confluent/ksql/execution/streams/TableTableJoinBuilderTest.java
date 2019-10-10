package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
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
  private static final SourceName LEFT = SourceName.of("LEFT");
  private static final SourceName RIGHT = SourceName.of("RIGHT");
  private static final SourceName ALIAS = SourceName.of("ALIAS");
  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.INTEGER)
      .build()
      .withAlias(LEFT)
      .withMetaAndKeyColsInValue();
  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
      .build()
      .withAlias(RIGHT)
      .withMetaAndKeyColsInValue();
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("GREEN"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORANGE"), SqlTypes.DOUBLE)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private final QueryContext SRC_CTX =
      new QueryContext.Stacker().push("src").getQueryContext();
  private final QueryContext CTX =
      new QueryContext.Stacker().push("jo").push("in").getQueryContext();

  @Mock
  private KTable<Struct, GenericRow> leftKTable;
  @Mock
  private KTable<Struct, GenericRow>  rightKTable;
  @Mock
  private KTable<Struct, GenericRow>  resultKTable;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> left;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;

  private PlanBuilder planBuilder;
  private TableTableJoin<Struct> join;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(left.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(LEFT_SCHEMA, SRC_CTX));
    when(right.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(RIGHT_SCHEMA, SRC_CTX));
    when(left.build(any())).thenReturn(
        KTableHolder.unmaterialized(leftKTable, keySerdeFactory));
    when(right.build(any())).thenReturn(
        KTableHolder.unmaterialized(rightKTable, keySerdeFactory));
    planBuilder = new KSPlanBuilder(
        mock(KsqlQueryBuilder.class),
        mock(SqlPredicateFactory.class),
        mock(AggregateParams.Factory.class),
        mock(StreamsFactories.class)
    );
  }

  @SuppressWarnings("unchecked")
  private void givenLeftJoin() {
    when(leftKTable.leftJoin(any(KTable.class), any())).thenReturn(resultKTable);
    join = new TableTableJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.LEFT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenOuterJoin() {
    when(leftKTable.outerJoin(any(KTable.class), any())).thenReturn(resultKTable);
    join = new TableTableJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.OUTER,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenInnerJoin() {
    when(leftKTable.join(any(KTable.class), any())).thenReturn(resultKTable);
    join = new TableTableJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.INNER,
        left,
        right
    );
  }

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin();

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder);

    // Then:
    verify(leftKTable).leftJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldDoOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder);

    // Then:
    verify(leftKTable).outerJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin();

    // When:
    final KTableHolder<Struct> result = join.build(planBuilder);

    // Then:
    verify(leftKTable).join(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftKTable, rightKTable, resultKTable);
    assertThat(result.getTable(), is(resultKTable));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }
}
