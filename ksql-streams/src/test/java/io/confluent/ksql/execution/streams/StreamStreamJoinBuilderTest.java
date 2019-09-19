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
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
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
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamStreamJoinBuilderTest {
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
  private static final PhysicalSchema RIGHT_PHYSICAL =
      PhysicalSchema.from(RIGHT_SCHEMA.withoutAlias(), SerdeOption.none());
  private static final Formats LEFT_FMT = Formats.of(
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.JSON)),
      SerdeOption.none()
  );
  private static final Formats RIGHT_FMT = Formats.of(
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.AVRO)),
      SerdeOption.none()
  );
  private static final Duration BEFORE = Duration.ofMillis(1000);
  private static final Duration AFTER = Duration.ofMillis(2000);
  private static final JoinWindows WINDOWS = JoinWindows.of(BEFORE).after(AFTER);
  private final QueryContext SRC_CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("src").getQueryContext();
  private final QueryContext CTX =
      new QueryContext.Stacker(new QueryId("qid")).push("jo").push("in").getQueryContext();

  @Mock
  private KStream<Struct, GenericRow> leftStream;
  @Mock
  private KStream<Struct, GenericRow>  rightStream;
  @Mock
  private KStream<Struct, GenericRow>  resultStream;
  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> left;
  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> right;
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
  @Mock
  private Serde<GenericRow> rightSerde;

  private StreamStreamJoin<Struct> join;

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
    when(queryBuilder.buildValueSerde(eq(FormatInfo.of(Format.AVRO)), any(), any()))
        .thenReturn(rightSerde);
    when(joinedFactory.create(any(Serde.class), any(), any(), any())).thenReturn(joined);
  }

  @SuppressWarnings("unchecked")
  private void givenLeftJoin() {
    when(leftStream.leftJoin(any(KStream.class), any(), any(), any())).thenReturn(resultStream);
    join = new StreamStreamJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.LEFT,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER
    );
  }

  @SuppressWarnings("unchecked")
  private void givenOuterJoin() {
    when(leftStream.outerJoin(any(KStream.class), any(), any(), any())).thenReturn(resultStream);
    join = new StreamStreamJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.OUTER,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER
    );
  }

  @SuppressWarnings("unchecked")
  private void givenInnerJoin() {
    when(leftStream.join(any(KStream.class), any(), any(), any())).thenReturn(resultStream);
    join = new StreamStreamJoin<>(
        new DefaultExecutionStepProperties(SCHEMA, CTX),
        JoinType.INNER,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER
    );
  }

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin();

    // When:
    final KStream result = StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(leftStream).leftJoin(
        same(rightStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        eq(WINDOWS),
        same(joined)
    );
    verifyNoMoreInteractions(leftStream, rightStream, resultStream);
    assertThat(result, is(resultStream));
  }

  @Test
  public void shouldDoOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    final KStream result = StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(leftStream).outerJoin(
        same(rightStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        eq(WINDOWS),
        same(joined)
    );
    verifyNoMoreInteractions(leftStream, rightStream, resultStream);
    assertThat(result, is(resultStream));
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin();

    // When:
    final KStream result = StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(leftStream).join(
        same(rightStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA, RIGHT_SCHEMA)),
        eq(WINDOWS),
        same(joined)
    );
    verifyNoMoreInteractions(leftStream, rightStream, resultStream);
    assertThat(result, is(resultStream));
  }

  @Test
  public void shouldBuildJoinedCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    verify(joinedFactory).create(keySerde, leftSerde, rightSerde, "jo-in");
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
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
    StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("left").getQueryContext();
    verify(queryBuilder).buildValueSerde(FormatInfo.of(Format.JSON), LEFT_PHYSICAL, leftCtx);
  }

  @Test
  public void shouldBuildRightSerdeCorrectly() {
    // Given:
    givenInnerJoin();

    // When:
    StreamStreamJoinBuilder.build(
        leftStream,
        rightStream,
        join,
        keySerdeFactory,
        queryBuilder,
        joinedFactory
    );

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("right").getQueryContext();
    verify(queryBuilder).buildValueSerde(FormatInfo.of(Format.AVRO), RIGHT_PHYSICAL, leftCtx);
  }
}