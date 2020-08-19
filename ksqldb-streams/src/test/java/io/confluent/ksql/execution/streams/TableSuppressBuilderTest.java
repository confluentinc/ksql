/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TableSuppressBuilderTest {

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ExecutionStep<KTableHolder<Struct>> sourceStep;
  @Mock
  private KTable<Struct, GenericRow> sourceKTable;
  @Mock
  private KTable<Struct, GenericRow> preKTable;
  @Mock
  private KTable<Struct, GenericRow> suppressedKTable;
  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private Formats internalFormats;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;
  @Mock
  private  PhysicalSchema physicalSchema;
  @Mock
  private  Serde<GenericRow> valueSerde;
  @Mock
  private  Serde<Struct> keySerde;
  @Mock
  private  Materialized<Object, GenericRow, StateStore> materialized;
  @Mock
  private KTableHolder<Struct> tableHolder;
  @Mock
  private KTableHolder<Struct> suppressedtable;
  @Captor
  private ArgumentCaptor<FinalResultsSuppressionBuilder> suppressionCaptor;

  private final QueryContext queryContext = new QueryContext.Stacker()
      .push("bar")
      .getQueryContext();

  private TableSuppress<Struct> tableSuppress;
  private BiFunction<LogicalSchema, SerdeOptions, PhysicalSchema> physicalSchemaFactory;
  private BiFunction<Serde<Struct>, Serde<GenericRow>, Materialized> materializedFactory;
  private Long maxBytes = 300L;
  private TableSuppressBuilder builder;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(queryContext);

    physicalSchemaFactory = (a,b) -> physicalSchema;
    materializedFactory = (a,b) -> materialized;

    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(keySerdeFactory.buildKeySerde(any(), any(), any())).thenReturn(keySerde);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlConfig.getLong(any())).thenReturn(maxBytes);

    when(tableHolder.getTable()).thenReturn(sourceKTable);
    when(sourceKTable.transformValues(any(), any(Materialized.class))).thenReturn(preKTable);
    when(preKTable.suppress(any())).thenReturn(suppressedKTable);
    when(tableHolder.withTable(any(),any())).thenReturn(suppressedtable);

    tableSuppress = new TableSuppress<>(properties, sourceStep, refinementInfo, internalFormats);
    builder = new TableSuppressBuilder();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSuppressSourceTable() {
    // When:
    final KTableHolder<Struct> result = builder.build(tableHolder, tableSuppress, queryBuilder, keySerdeFactory, physicalSchemaFactory, materializedFactory);

    // Then:
    assertThat(result, is(suppressedtable));
    verify(sourceKTable).transformValues(any(),any(Materialized.class));
    verify(preKTable).suppress(suppressionCaptor.capture());
    final FinalResultsSuppressionBuilder suppression = suppressionCaptor.getValue();
    assertThat(suppression, isA(FinalResultsSuppressionBuilder.class));
  }
}

