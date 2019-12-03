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
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamTableJoinBuilderTest {
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
  private static final PhysicalSchema LEFT_PHYSICAL =
      PhysicalSchema.from(LEFT_SCHEMA.withoutAlias(), SerdeOption.none());
  private static final Formats LEFT_FMT = Formats.of(
      FormatInfo.of(Format.KAFKA),
      FormatInfo.of(Format.JSON),
      SerdeOption.none()
  );
  private final QueryContext CTX =
      new QueryContext.Stacker().push("jo").push("in").getQueryContext();

  @Mock
  private KStream<Struct, GenericRow> leftKStream;
  @Mock
  private KTable<Struct, GenericRow> rightKTable;
  @Mock
  private KStream<Struct, GenericRow> resultStream;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> right;
  @Mock
  private Joined<Struct, GenericRow, GenericRow> joined;
  @Mock
  private JoinedFactory joinedFactory;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;

  private PlanBuilder planBuilder;
  private StreamTableJoin<Struct> join;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(eq(FormatInfo.of(Format.JSON)), any(), any()))
        .thenReturn(leftSerde);
    when(joinedFactory.create(any(Serde.class), any(), any(), any())).thenReturn(joined);
    when(left.build(any())).thenReturn(
        new KStreamHolder<>(leftKStream, LEFT_SCHEMA, keySerdeFactory));
    when(right.build(any())).thenReturn(
        KTableHolder.unmaterialized(rightKTable, RIGHT_SCHEMA, keySerdeFactory));
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        new StreamsFactories(
            mock(GroupedFactory.class),
            joinedFactory,
            mock(MaterializedFactory.class),
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  @SuppressWarnings("unchecked")
  private void givenLeftJoin() {
    when(leftKStream.leftJoin(any(KTable.class), any(), any())).thenReturn(resultStream);
    join = new StreamTableJoin(
        new ExecutionStepPropertiesV1(SCHEMA, CTX),
        JoinType.LEFT,
        LEFT_FMT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenOuterJoin() {
    join = new StreamTableJoin(
        new ExecutionStepPropertiesV1(SCHEMA, CTX),
        JoinType.OUTER,
        LEFT_FMT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenInnerJoin() {
    when(leftKStream.join(any(KTable.class), any(), any())).thenReturn(resultStream);
    join = new StreamTableJoin(
        new ExecutionStepPropertiesV1(SCHEMA, CTX),
        JoinType.INNER,
        LEFT_FMT,
        left,
        right
    );
  }

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin();

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder);

    // Then:
    verify(leftKStream).leftJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shoulFailOnOuterJoin() {
    // Given:
    givenOuterJoin();

    // Then:
    expectedException.expect(IllegalStateException.class);

    // When:
    join.build(planBuilder);
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin();

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder);

    // Then:
    verify(leftKStream).join(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // Given:
    givenInnerJoin();

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder);

    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  @Test
  public void shouldBuildJoinedCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    join.build(planBuilder);

    // Then:
    verify(joinedFactory).create(keySerde, leftSerde, null, "jo-in");
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    join.build(planBuilder);

    // Then:
    verify(keySerdeFactory).buildKeySerde(LEFT_FMT.getKeyFormat(), LEFT_PHYSICAL, CTX);
  }

  @Test
  public void shouldBuildLeftSerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    join.build(planBuilder);

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("left").getQueryContext();
    verify(queryBuilder).buildValueSerde(FormatInfo.of(Format.JSON), LEFT_PHYSICAL, leftCtx);
  }
}
