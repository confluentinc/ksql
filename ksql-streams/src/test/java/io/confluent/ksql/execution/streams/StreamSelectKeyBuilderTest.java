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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
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
  private static final String ALIAS = "ATL";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("BIG", SqlTypes.BIGINT)
      .valueColumn("BOI", SqlTypes.STRING)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private static final String KEY = "ATL.BOI";

  @Mock
  private KStream<Struct, GenericRow> kstream;
  @Mock
  private KStream<Struct, GenericRow> rekeyedKstream;
  @Mock
  private KStream<Struct, GenericRow> filteredKStream;
  @Mock
  private KStream<Struct, GenericRow> updatedKeyKStream;
  @Mock
  private ExecutionStep<KStream<Struct, GenericRow>> sourceStep;
  @Captor
  private ArgumentCaptor<Predicate<Struct, GenericRow>> predicateCaptor;
  @Captor
  private ArgumentCaptor<KeyValueMapper<Struct, GenericRow, Struct>> keyValueMapperCaptor;
  @Captor
  private ArgumentCaptor<ValueMapperWithKey<Struct, GenericRow, GenericRow>> mapperCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker(new QueryId("hey")).push("ya").getQueryContext();
  private final ExecutionStepProperties properties = new DefaultExecutionStepProperties(
      SCHEMA,
      queryContext
  );

  private StreamSelectKey selectKey;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(sourceStep.getProperties()).thenReturn(properties);
    when(kstream.filter(any())).thenReturn(filteredKStream);
    when(filteredKStream.selectKey(any(KeyValueMapper.class))).thenReturn(rekeyedKstream);
    when(rekeyedKstream.mapValues(any(ValueMapperWithKey.class))).thenReturn(updatedKeyKStream);
    givenUpdateRowkey();
  }

  private void givenUpdateRowkey() {
    selectKey = new StreamSelectKey<>(
        properties,
        sourceStep,
        KEY,
        true
    );
  }

  private void givenUpdateRowkeyFalse() {
    selectKey = new StreamSelectKey<>(
        properties,
        sourceStep,
        KEY,
        false
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRekeyCorrectly() {
    // When:
    final KStream result = StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    final InOrder inOrder = Mockito.inOrder(kstream, filteredKStream, rekeyedKstream);
    inOrder.verify(kstream).filter(any());
    inOrder.verify(filteredKStream).selectKey(any());
    inOrder.verify(rekeyedKstream).mapValues(any(ValueMapperWithKey.class));
    inOrder.verifyNoMoreInteractions();
    assertThat(result, is(updatedKeyKStream));
  }

  @Test
  public void shouldFilterOutNullValues() {
    // When:
    StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(asStructKey("dre"), null), is(false));
  }

  @Test
  public void shouldFilterOutNullKeyColumns() {
    // When:
    StreamSelectKeyBuilder.build(kstream, selectKey);

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
    StreamSelectKeyBuilder.build(kstream, selectKey);

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
    StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<Struct, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(asStructKey("dre"), new GenericRow(0, "dre", null, "bob")), is(true));
  }

  @Test
  public void shouldComputeCorrectKey() {
    // When:
    StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    final KeyValueMapper<Struct, GenericRow, Struct> keyValueMapper = getKeyMapper();
    assertThat(
        keyValueMapper.apply(asStructKey("dre"), new GenericRow(0, "dre", 3000, "bob")),
        is(asStructKey("bob"))
    );
  }

  @Test
  public void shouldUpdateRowkeyIfUpdateRowkeyTrue() {
    // When:
    StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    verify(rekeyedKstream).mapValues(mapperCaptor.capture());
    final ValueMapperWithKey<Struct, GenericRow, GenericRow> mapper = getMapper();
    assertThat(
        mapper.apply(asStructKey("bob"), new GenericRow(0, "dre", 3000, "bob")),
        equalTo(new GenericRow(0, "bob", 3000, "bob"))
    );
  }

  @Test
  public void shouldNotUpdateRowkeyIfUpdateRowkeyTrue() {
    // Given:
    givenUpdateRowkeyFalse();

    // When:
    StreamSelectKeyBuilder.build(kstream, selectKey);

    // Then:
    final ValueMapperWithKey<Struct, GenericRow, GenericRow> mapper = getMapper();
    assertThat(
        mapper.apply(asStructKey("bob"), new GenericRow(0, "dre", 3000, "bob")),
        equalTo(new GenericRow(0, "dre", 3000, "bob"))
    );
  }

  private KeyValueMapper<Struct, GenericRow, Struct> getKeyMapper() {
    verify(filteredKStream).selectKey(keyValueMapperCaptor.capture());
    return keyValueMapperCaptor.getValue();
  }

  private ValueMapperWithKey<Struct, GenericRow, GenericRow> getMapper() {
    verify(rekeyedKstream).mapValues(mapperCaptor.capture());
    return mapperCaptor.getValue();
  }

  private Predicate<Struct, GenericRow> getPredicate() {
    verify(kstream).filter(predicateCaptor.capture());
    return predicateCaptor.getValue();
  }
}