package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableTableJoinBuilderTest {
  private static final String LEFT = "LEFT";
  private static final String RIGHT = "RIGHT";
  private static final String ALIAS = "ALIAS";
  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .valueColumn("BLUE", SqlTypes.STRING)
      .valueColumn("GREEN", SqlTypes.INTEGER)
      .build()
      .withAlias(LEFT)
      .withMetaAndKeyColsInValue();
  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .valueColumn("RED", SqlTypes.BIGINT)
      .valueColumn("ORANGE", SqlTypes.DOUBLE)
      .build()
      .withAlias(RIGHT)
      .withMetaAndKeyColsInValue();
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("BLUE", SqlTypes.STRING)
      .valueColumn("GREEN", SqlTypes.STRING)
      .valueColumn("RED", SqlTypes.BIGINT)
      .valueColumn("ORANGE", SqlTypes.DOUBLE)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private final QueryContext SRC_CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("src").getQueryContext();
  private final QueryContext CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("jo").push("in").getQueryContext();

  @Mock
  private KTable<Struct, GenericRow> leftTable;
  @Mock
  private KTable<Struct, GenericRow>  rightTable;
  @Mock
  private KTable<Struct, GenericRow>  resultTable;
  @Mock
  private ExecutionStep<KTable<Struct, GenericRow>> left;
  @Mock
  private ExecutionStep<KTable<Struct, GenericRow>> right;
  @Mock
  private Joined<Struct, GenericRow, GenericRow> joined;
  @Mock
  private JoinedFactory joinedFactory;

  private TableTableJoin<Struct> join;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(left.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(LEFT_SCHEMA, SRC_CTX));
    when(right.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(RIGHT_SCHEMA, SRC_CTX));
    when(joinedFactory.create(any(Serde.class), any(), any(), any())).thenReturn(joined);
  }

  @SuppressWarnings("unchecked")
  private void givenLeftJoin() {
    when(leftTable.leftJoin(any(KTable.class), any())).thenReturn(resultTable);
    join = new TableTableJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.LEFT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenOuterJoin() {
    when(leftTable.outerJoin(any(KTable.class), any())).thenReturn(resultTable);
    join = new TableTableJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.OUTER,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenInnerJoin() {
    when(leftTable.join(any(KTable.class), any())).thenReturn(resultTable);
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
    final KTable result = TableTableJoinBuilder.build(leftTable, rightTable, join);

    // Then:
    verify(leftTable).leftJoin(
        same(rightTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftTable, rightTable, resultTable);
    assertThat(result, is(resultTable));
  }

  @Test
  public void shouldDoOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    final KTable result = TableTableJoinBuilder.build(leftTable, rightTable, join);

    // Then:
    verify(leftTable).outerJoin(
        same(rightTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftTable, rightTable, resultTable);
    assertThat(result, is(resultTable));
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin();

    // When:
    final KTable result = TableTableJoinBuilder.build(leftTable, rightTable, join);

    // Then:
    verify(leftTable).join(
        same(rightTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA))
    );
    verifyNoMoreInteractions(leftTable, rightTable, resultTable);
    assertThat(result, is(resultTable));
  }
}