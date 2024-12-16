/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import static io.confluent.ksql.execution.plan.StreamStreamJoin.LEGACY_KEY_COL;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWKEY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// Can be fixed after GRACE is mandatory
@SuppressWarnings("deprecation")
@RunWith(MockitoJUnitRunner.class)
public class StreamStreamJoinBuilderTest {

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

  private static final PhysicalSchema LEFT_PHYSICAL =
      PhysicalSchema.from(LEFT_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final PhysicalSchema RIGHT_PHYSICAL =
      PhysicalSchema.from(RIGHT_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final Formats LEFT_FMT = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.JSON.name()),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  private static final Formats RIGHT_FMT = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.AVRO.name()),
      SerdeFeatures.of(),
      SerdeFeatures.of()
  );

  private static final Duration BEFORE = Duration.ofMillis(1000);
  private static final Duration AFTER = Duration.ofMillis(2000);
  private static final Duration GRACE = Duration.ofMillis(3000);
  @SuppressWarnings("deprecation") // can be fixed after GRACE clause is made mandatory
  private static final JoinWindows WINDOWS_NO_GRACE = JoinWindows
      .of(BEFORE)
      .after(AFTER);
  @SuppressWarnings("deprecation") // can be fixed after GRACE clause is made mandatory
  private static final JoinWindows WINDOWS_WITH_GRACE = JoinWindows
      .of(BEFORE)
      .after(AFTER)
      .grace(GRACE);

  private final QueryContext CTX =
      new QueryContext.Stacker().push("jo").push("in").getQueryContext();

  @Mock
  private KStream<Struct, GenericRow> leftKStream;
  @Mock
  private KStream<Struct, GenericRow> rightKStream;
  @Mock
  private KStream<Struct, GenericRow> resultKStream;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> left;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> right;
  @Mock
  private StreamJoined<Struct, GenericRow, GenericRow> joined;
  @Mock
  private StreamJoinedFactory streamJoinedFactory;
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;
  @Mock
  private Serde<GenericRow> rightSerde;

  private PlanBuilder planBuilder;
  private StreamStreamJoin<Struct> join;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(executionKeyFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(eq(FormatInfo.of(FormatFactory.JSON.name())), any(), any()))
        .thenReturn(leftSerde);
    when(buildContext.buildValueSerde(eq(FormatInfo.of(FormatFactory.AVRO.name())), any(), any()))
        .thenReturn(rightSerde);
    when(streamJoinedFactory.create(any(Serde.class), any(Serde.class), any(Serde.class), anyString(), anyString())).thenReturn(joined);
    when(left.build(any(), eq(planInfo))).thenReturn(
        new KStreamHolder<>(leftKStream, LEFT_SCHEMA, executionKeyFactory));
    when(right.build(any(), eq(planInfo))).thenReturn(
        new KStreamHolder<>(rightKStream, RIGHT_SCHEMA, executionKeyFactory));

    when(leftKStream.leftJoin(any(KStream.class), any(KsqlValueJoiner.class), any(), any(StreamJoined.class))).thenReturn(resultKStream);
    when(leftKStream.outerJoin(any(KStream.class), any(KsqlValueJoiner.class), any(), any(StreamJoined.class))).thenReturn(resultKStream);
    when(leftKStream.join(any(KStream.class), any(KsqlValueJoiner.class), any(), any(StreamJoined.class))).thenReturn(resultKStream);

    planBuilder = new KSPlanBuilder(
        buildContext,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            mock(MaterializedFactory.class),
            streamJoinedFactory,
            mock(ConsumedFactory.class)
        )
    );
  }

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin(L_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).leftJoin(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        eq(WINDOWS_NO_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoLeftJoinWithSyntheticKey() {
    // Given:
    givenLeftJoin(SYNTH_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).leftJoin(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        eq(WINDOWS_NO_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoLeftJoinWithGrace() {
    // Given:
    givenLeftJoin(L_KEY, Optional.of(GRACE));

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).leftJoin(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        eq(WINDOWS_WITH_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).outerJoin(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        eq(WINDOWS_NO_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoOuterJoinWithGrace() {
    // Given:
    givenOuterJoin(Optional.of(GRACE));

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).outerJoin(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        eq(WINDOWS_WITH_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).join(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        eq(WINDOWS_NO_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoInnerJoinWithSyntheticKey() {
    // Given:
    givenInnerJoin(SYNTH_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).join(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        eq(WINDOWS_NO_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldDoInnerJoinWithGrace() {
    // Given:
    givenInnerJoin(L_KEY, Optional.of(GRACE));

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).join(
        same(rightKStream),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        eq(WINDOWS_WITH_GRACE),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKStream, resultKStream);
    assertThat(result.getStream(), is(resultKStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // Given:
    givenInnerJoin(R_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(R_KEY, LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  @Test
  public void shouldReturnCorrectLegacySchema() {
    // Given:
    givenInnerJoin(L_KEY);

    join = new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.INNER,
        ColumnName.of(LEGACY_KEY_COL),
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER,
        Optional.empty()
    );

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    assertThat(
        result.getSchema(),
        is(JoinParamsFactory.create(ROWKEY_NAME, LEFT_SCHEMA, RIGHT_SCHEMA).getSchema())
    );
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldBuildJoinedCorrectly() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(streamJoinedFactory).create(keySerde, leftSerde, rightSerde, "jo-in", "jo-in");
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(executionKeyFactory).buildKeySerde(LEFT_FMT.getKeyFormat(), LEFT_PHYSICAL, CTX);
  }

  @Test
  public void shouldBuildLeftSerdeCorrectly() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("Left").getQueryContext();
    verify(buildContext).buildValueSerde(FormatInfo.of(FormatFactory.JSON.name()), LEFT_PHYSICAL, leftCtx);
  }

  @Test
  public void shouldBuildRightSerdeCorrectly() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("Right").getQueryContext();
    verify(buildContext).buildValueSerde(FormatInfo.of(FormatFactory.AVRO.name()), RIGHT_PHYSICAL, leftCtx);
  }

  private void givenLeftJoin(final ColumnName keyName) {
    givenLeftJoin(keyName, Optional.empty());
  }

  private void givenLeftJoin(final ColumnName keyName, final Optional<Duration> grace) {
    join = new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.LEFT,
        keyName,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER,
        grace
    );
  }

  private void givenOuterJoin() {
    givenOuterJoin(Optional.empty());
  }

  private void givenOuterJoin(final Optional<Duration> grace) {
    join = new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.OUTER,
        SYNTH_KEY,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER,
        grace
    );
  }

  private void givenInnerJoin(final ColumnName keyName) {
    givenInnerJoin(keyName, Optional.empty());
  }

  private void givenInnerJoin(final ColumnName keyName, final Optional<Duration> grace) {
    join = new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.INNER,
        keyName,
        LEFT_FMT,
        RIGHT_FMT,
        left,
        right,
        BEFORE,
        AFTER,
        grace
    );
  }
}
