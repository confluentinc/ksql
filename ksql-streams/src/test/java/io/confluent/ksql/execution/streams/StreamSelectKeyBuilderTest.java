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

import static io.confluent.ksql.execution.util.StructKeyUtil.asStructKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class StreamSelectKeyBuilderTest {
  private static final SourceName ALIAS = SourceName.of("ATL");
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.STRING)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private static final ColumnRef KEY = ColumnRef.of(SourceName.of("ATL"), ColumnName.of("BOI"));

  @Mock
  private KStream<Struct, GenericRow> kstream;
  @Mock
  private KStream<Struct, GenericRow> rekeyedKstream;
  @Mock
  private KStream<Struct, GenericRow> filteredKStream;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> sourceStep;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Captor
  private ArgumentCaptor<Predicate<Struct, GenericRow>> predicateCaptor;
  @Captor
  private ArgumentCaptor<KeyValueMapper<Struct, GenericRow, Struct>> keyValueMapperCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker().push("ya").getQueryContext();
  private final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(queryContext);

  private PlanBuilder planBuilder;
  private StreamSelectKey selectKey;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("hey"));
    when(sourceStep.getProperties()).thenReturn(properties);
    when(kstream.filter(any())).thenReturn(filteredKStream);
    when(filteredKStream.selectKey(any(KeyValueMapper.class))).thenReturn(rekeyedKstream);
    when(sourceStep.build(any())).thenReturn(
        new KStreamHolder<>(kstream, SCHEMA, mock(KeySerdeFactory.class)));
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
    selectKey = new StreamSelectKey(
        properties,
        sourceStep,
        KEY
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRekeyCorrectly() {
    // When:
    final KStreamHolder<Struct> result = selectKey.build(planBuilder);

    // Then:
    final InOrder inOrder = Mockito.inOrder(kstream, filteredKStream, rekeyedKstream);
    inOrder.verify(kstream).filter(any());
    inOrder.verify(filteredKStream).selectKey(any());
    inOrder.verifyNoMoreInteractions();
    assertThat(result.getStream(), is(rekeyedKstream));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnCorrectSerdeFactory() {
    // When:
    final KStreamHolder<Struct> result = selectKey.build(planBuilder);

    // Then:
    result.getKeySerdeFactory().buildKeySerde(
        FormatInfo.of(Format.JSON),
        PhysicalSchema.from(SCHEMA, SerdeOption.none()),
        queryContext
    );
    verify(queryBuilder).buildKeySerde(
        FormatInfo.of(Format.JSON),
        PhysicalSchema.from(SCHEMA, SerdeOption.none()),
        queryContext);
  }

  @Test
  public void shouldFilterOutNullValues() {
    // When:
    selectKey.build(planBuilder);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(asStructKey("dre"), null), is(false));
  }

  @Test
  public void shouldFilterOutNullKeyColumns() {
    // When:
    selectKey.build(planBuilder);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(
        predicate.test(asStructKey("dre"), new GenericRow(0, "dre", 3000, null)),
        is(false)
    );
  }

  @Test
  public void shouldNotFilterOutNonNullKeyColumns() {
    // When:
    selectKey.build(planBuilder);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(
        predicate.test(asStructKey("dre"), new GenericRow(0, "dre", 3000, "bob")),
        is(true)
    );
  }

  @Test
  public void shouldIgnoreNullNonKeyColumns() {
    // When:
    selectKey.build(planBuilder);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(asStructKey("dre"), new GenericRow(0, "dre", null, "bob")), is(true));
  }

  @Test
  public void shouldComputeCorrectKey() {
    // When:
    selectKey.build(planBuilder);

    // Then:
    final KeyValueMapper<Struct, GenericRow, Struct> keyValueMapper = getKeyMapper();
    assertThat(
        keyValueMapper.apply(asStructKey("dre"), new GenericRow(0, "dre", 3000, "bob")),
        is(asStructKey("bob"))
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<Struct> result = selectKey.build(planBuilder);

    // Then:
    assertThat(result.getSchema(), is(SCHEMA));
  }

  private KeyValueMapper<Struct, GenericRow, Struct> getKeyMapper() {
    verify(filteredKStream).selectKey(keyValueMapperCaptor.capture());
    return keyValueMapperCaptor.getValue();
  }

  private Predicate<Struct, GenericRow> getPredicate() {
    verify(kstream).filter(predicateCaptor.capture());
    return predicateCaptor.getValue();
  }
}
