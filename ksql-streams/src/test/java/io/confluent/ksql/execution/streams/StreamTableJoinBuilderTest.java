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
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
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
  private static final PhysicalSchema LEFT_PHYSICAL =
      PhysicalSchema.from(LEFT_SCHEMA.withoutAlias(), SerdeOption.none());
  private static final Formats LEFT_FMT = Formats.of(
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.JSON)),
      SerdeOption.none()
  );
  private final QueryContext SRC_CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("src").getQueryContext();
  private final QueryContext CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("jo").push("in").getQueryContext();

  @Mock
  private KStream<Struct, GenericRow> leftStream;
  @Mock
  private KTable<Struct, GenericRow> rightTable;
  @Mock
  private KStream<Struct, GenericRow> resultStream;
  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> left;
  @Mock
  private ExecutionStep<KTable<Struct, GenericRow>> right;
  @Mock
  private Joined<Struct, GenericRow, GenericRow> joined;
  @Mock
  private JoinedFactory joinedFactory;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;

  private StreamTableJoin<Struct> join;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(left.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(LEFT_SCHEMA, SRC_CTX));
    when(right.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(RIGHT_SCHEMA, SRC_CTX));
    when(keySerdeFactory.buildKeySerde(any(KeyFormat.class), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(eq(FormatInfo.of(Format.JSON)), any(), any()))
        .thenReturn(leftSerde);
    when(joinedFactory.create(any(Serde.class), any(), any(), any())).thenReturn(joined);
  }

  @SuppressWarnings("unchecked")
  private void givenLeftJoin() {
    when(leftStream.leftJoin(any(KTable.class), any(), any())).thenReturn(resultStream);
    join = new StreamTableJoin(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.LEFT,
        LEFT_FMT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenOuterJoin() {
    join = new StreamTableJoin(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.OUTER,
        LEFT_FMT,
        left,
        right
    );
  }

  @SuppressWarnings("unchecked")
  private void givenInnerJoin() {
    when(leftStream.join(any(KTable.class), any(), any())).thenReturn(resultStream);
    join = new StreamTableJoin(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
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
    final KStream result = StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(leftStream).leftJoin(
        same(rightTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        same(joined)
    );
    verifyNoMoreInteractions(leftStream, rightTable, resultStream);
    assertThat(result, is(resultStream));
  }

  @Test
  public void shoulFailOnOuterJoin() {
    // Given:
    givenOuterJoin();

    // Then:
    expectedException.expect(IllegalStateException.class);

    // When:
    StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin();

    // When:
    final KStream result = StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(leftStream).join(
        same(rightTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        same(joined)
    );
    verifyNoMoreInteractions(leftStream, rightTable, resultStream);
    assertThat(result, is(resultStream));
  }

  @Test
  public void shouldBuildJoinedCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(joinedFactory).create(keySerde, leftSerde, null, "jo-in");
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(keySerdeFactory).buildKeySerde(LEFT_FMT.getKeyFormat(), LEFT_PHYSICAL, CTX);
  }

  @Test
  public void shouldBuildLeftSerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamTableJoinBuilder.build(
        leftStream,
        rightTable,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("left").getQueryContext();
    verify(queryBuilder).buildValueSerde(FormatInfo.of(Format.JSON), LEFT_PHYSICAL, leftCtx);
  }
}