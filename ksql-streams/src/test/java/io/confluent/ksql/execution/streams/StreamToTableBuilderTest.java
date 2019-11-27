/*
 * Copyright 2019 Confluent Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamToTable;
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
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamToTableBuilderTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("PING"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("PONG"), SqlTypes.INTEGER)
      .build()
      .withAlias(SourceName.of("PADDLE"))
      .withMetaAndKeyColsInValue();

  @Mock
  private KStream<Struct, GenericRow> kStream;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized;
  @Mock
  private KGroupedStream<Struct, GenericRow> kGroupedStream;
  @Mock
  private KTable<Struct, GenericRow> kTable;
  @Mock
  private KsqlQueryBuilder ksqlQueryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> source;

  private final QueryContext.Stacker stacker = new QueryContext.Stacker();
  private final QueryContext queryContext = stacker.push("s2t").getQueryContext();
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private final PhysicalSchema physicalSchema = PhysicalSchema.from(
      SCHEMA,
      SerdeOption.none()
  );

  private PlanBuilder planBuilder;
  private StreamToTable<Struct> step;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(source.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(SCHEMA, stacker.push("source").getQueryContext())
    );
    when(materializedFactory.create(any(Serde.class), any(), any()))
        .thenReturn(materialized);
    when(kStream.mapValues(any(ValueMapper.class))).thenReturn(kStream);
    when(kStream.groupByKey()).thenReturn(kGroupedStream);
    when(kGroupedStream.aggregate(any(), any(), any(Materialized.class))).thenReturn(kTable);
    when(ksqlQueryBuilder.getQueryId()).thenReturn(new QueryId("qid"));
    when(ksqlQueryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(source.build(any())).thenReturn(
        new KStreamHolder<>(kStream, SCHEMA, keySerdeFactory));
    planBuilder = new KSPlanBuilder(
        ksqlQueryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        new StreamsFactories(
            mock(GroupedFactory.class),
            mock(JoinedFactory.class),
            materializedFactory,
            mock(StreamJoinedFactory.class),
            mock(ConsumedFactory.class)
        )
    );
  }

  private void givenUnwindowed() {
    step = new StreamToTable<>(
        source,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        new DefaultExecutionStepProperties(SCHEMA, queryContext)
    );
    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldConvertToTableCorrectly() {
    // Given:
    givenUnwindowed();

    // When:
    final KTableHolder<Struct> result = step.build(planBuilder);

    // Then:
    final InOrder inOrder = Mockito.inOrder(kStream);
    inOrder.verify(kStream).mapValues(any(ValueMapper.class));
    inOrder.verify(kStream).groupByKey();
    verify(kGroupedStream).aggregate(any(), any(), same(materialized));
    assertThat(result.getTable(), is(kTable));
    assertThat(result.getKeySerdeFactory(), is(keySerdeFactory));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // Given:
    givenUnwindowed();

    // When:
    final KTableHolder<Struct> result = step.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(SCHEMA));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildKeySerdeCorrectly() {
    // Given:
    givenUnwindowed();

    // When:
    step.build(planBuilder);

    // Then:
    verify(keySerdeFactory).buildKeySerde(
        keyFormat,
        physicalSchema,
        queryContext
    );
    verify(materializedFactory).create(same(keySerde), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildValueSerdeCorrectly() {
    // Given:
    givenUnwindowed();

    // When:
    step.build(planBuilder);

    // Then:
    verify(ksqlQueryBuilder).buildValueSerde(
        valueFormat.getFormatInfo(),
        physicalSchema,
        queryContext
    );
    verify(materializedFactory).create(any(), same(valueSerde), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldUseCorrectNameForMaterialized() {
    // Given:
    givenUnwindowed();

    // When:
    step.build(planBuilder);

    // Then:
    verify(materializedFactory).create(any(), any(), eq(StreamsUtil.buildOpName(queryContext)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldConvertToOptionalBeforeGroupingInToTable() {
    // Given:
    givenUnwindowed();

    // When:
    step.build(planBuilder);

    // Then:
    final ArgumentCaptor<ValueMapper> captor = ArgumentCaptor.forClass(ValueMapper.class);
    verify(kStream).mapValues(captor.capture());
    MatcherAssert.assertThat(captor.getValue().apply(null), equalTo(Optional.empty()));
    final GenericRow nonNull = new GenericRow(1, 2, 3);
    MatcherAssert.assertThat(captor.getValue().apply(nonNull), equalTo(Optional.of(nonNull)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldComputeAggregateCorrectlyInToTable() {
    // Given:
    givenUnwindowed();

    // When:
    step.build(planBuilder);

    // Then:
    final ArgumentCaptor<Initializer> initCaptor = ArgumentCaptor.forClass(Initializer.class);
    final ArgumentCaptor<Aggregator> captor = ArgumentCaptor.forClass(Aggregator.class);
    verify(kGroupedStream).aggregate(initCaptor.capture(), captor.capture(), any());
    assertThat(initCaptor.getValue().apply(), is(nullValue()));
    assertThat(captor.getValue().apply(null, Optional.empty(), null), is(nullValue()));
    final GenericRow nonNull = new GenericRow(1, 2, 3);
    assertThat(captor.getValue().apply(null, Optional.of(nonNull), null), is(nonNull));
  }
}
