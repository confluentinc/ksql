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

import io.confluent.ksql.execution.materialization.MaterializationInfo;
import static io.confluent.ksql.execution.plan.StreamStreamJoin.LEGACY_KEY_COL;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWKEY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamTableJoinBuilderTest {

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

  private static final Formats LEFT_FMT = Formats.of(
      FormatInfo.of(FormatFactory.KAFKA.name()),
      FormatInfo.of(FormatFactory.JSON.name()),
      SerdeFeatures.of(),
      SerdeFeatures.of()
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
  private RuntimeBuildContext buildContext;
  @Mock
  private ExecutionKeyFactory<Struct> executionKeyFactory;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;
  @Mock
  private PlanInfo planInfo;
  @Mock
  private MaterializationInfo.Builder materializationBuilder;

  private PlanBuilder planBuilder;
  private StreamTableJoin<Struct> join;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(executionKeyFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(buildContext.buildValueSerde(eq(FormatInfo.of(FormatFactory.JSON.name())), any(), any()))
        .thenReturn(leftSerde);
    when(joinedFactory.create(any(Serde.class), any(), any(), any())).thenReturn(joined);
    when(left.build(any(), eq(planInfo))).thenReturn(
        new KStreamHolder<>(leftKStream, LEFT_SCHEMA, executionKeyFactory));
    when(right.build(any(), eq(planInfo))).thenReturn(
        KTableHolder.materialized(rightKTable, RIGHT_SCHEMA, executionKeyFactory, materializationBuilder));

    when(leftKStream.leftJoin(any(KTable.class), any(KsqlValueJoiner.class), any()))
        .thenReturn(resultStream);
    when(leftKStream.join(any(KTable.class), any(KsqlValueJoiner.class), any()))
        .thenReturn(resultStream);

    planBuilder = new KSPlanBuilder(
        buildContext,
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

  @Test
  public void shouldDoLeftJoin() {
    // Given:
    givenLeftJoin(L_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).leftJoin(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
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
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
    assertThat(result.getExecutionKeyFactory(), is(executionKeyFactory));
  }

  @Test
  public void shouldFailOnOuterJoin() {
    // Given:
    givenOuterJoin();

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> join.build(planBuilder, planInfo)
    );
  }

  @Test
  public void shouldDoInnerJoin() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    final KStreamHolder<Struct> result = join.build(planBuilder, planInfo);

    // Then:
    verify(leftKStream).join(
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 0)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
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
        same(rightKTable),
        eq(new KsqlValueJoiner(LEFT_SCHEMA.value().size(), RIGHT_SCHEMA.value().size(), 1)),
        same(joined)
    );
    verifyNoMoreInteractions(leftKStream, rightKTable, resultStream);
    assertThat(result.getStream(), is(resultStream));
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
    join = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.INNER,
        ColumnName.of(LEGACY_KEY_COL),
        LEFT_FMT,
        left,
        right
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
  public void shouldBuildJoinedCorrectly() {
    // Given:
    givenInnerJoin(L_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(joinedFactory).create(keySerde, leftSerde, null, "jo-in");
  }

  @Test
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenInnerJoin(R_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    verify(executionKeyFactory).buildKeySerde(LEFT_FMT.getKeyFormat(), LEFT_PHYSICAL, CTX);
  }

  @Test
  public void shouldBuildLeftSerdeCorrectly() {
    // Given:
    givenInnerJoin(SYNTH_KEY);

    // When:
    join.build(planBuilder, planInfo);

    // Then:
    final QueryContext leftCtx = QueryContext.Stacker.of(CTX).push("Left").getQueryContext();
    verify(buildContext).buildValueSerde(FormatInfo.of(FormatFactory.JSON.name()), LEFT_PHYSICAL, leftCtx);
  }

  private void givenLeftJoin(final ColumnName keyName) {
    join = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.LEFT,
        keyName,
        LEFT_FMT,
        left,
        right
    );
  }

  private void givenOuterJoin() {
    join = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.OUTER,
        SYNTH_KEY,
        LEFT_FMT,
        left,
        right
    );
  }

  private void givenInnerJoin(final ColumnName keyName) {
    join = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(CTX),
        JoinType.INNER,
        keyName,
        LEFT_FMT,
        left,
        right
    );
  }
}
