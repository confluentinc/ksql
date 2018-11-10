/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.structured;

import static java.util.Collections.emptyMap;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.streams.KsqlMaterializedFactory;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.Windows;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class SchemaKGroupedStreamTest {
  private static final String AGGREGATE_OP_NAME = "AGGREGATE";

  @Mock(MockType.NICE)
  private Schema schema;
  @Mock(MockType.NICE)
  private KGroupedStream groupedStream;
  @Mock(MockType.NICE)
  private Field keyField;
  @Mock(MockType.NICE)
  private List<SchemaKStream> sourceStreams;
  @Mock(MockType.NICE)
  private KsqlConfig config;
  @Mock(MockType.NICE)
  private FunctionRegistry funcRegistry;
  @Mock(MockType.NICE)
  private SchemaRegistryClient srClient;
  @Mock(MockType.NICE)
  private Initializer initializer;
  @Mock(MockType.NICE)
  private Serde<GenericRow> topicValueSerDe;
  @Mock(MockType.NICE)
  private KsqlAggregateFunction windowStartFunc;
  @Mock(MockType.NICE)
  private KsqlAggregateFunction windowEndFunc;
  @Mock(MockType.NICE)
  private KsqlAggregateFunction otherFunc;
  @Mock(MockType.NICE)
  private KTable table;
  @Mock(MockType.NICE)
  private KTable table2;
  @Mock(MockType.NICE)
  private WindowExpression windowExp;
  @Mock(MockType.NICE)
  private KsqlWindowExpression ksqlWindowExp;
  @Mock(MockType.NICE)
  private Serde<Windowed<String>> windowedKeySerde;
  @Mock(MockType.NICE)
  private MaterializedFactory materializedFactory;
  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        schema, groupedStream, keyField, sourceStreams, config, funcRegistry, srClient,
        materializedFactory);

    EasyMock.expect(windowStartFunc.getFunctionName()).andReturn("WindowStart").anyTimes();
    EasyMock.expect(windowEndFunc.getFunctionName()).andReturn("WindowEnd").anyTimes();
    EasyMock.expect(otherFunc.getFunctionName()).andReturn("NotWindowStartFunc").anyTimes();
    EasyMock.expect(windowExp.getKsqlWindowExpression()).andReturn(ksqlWindowExp).anyTimes();
    EasyMock.expect(ksqlWindowExp.getKeySerde(String.class)).andReturn(windowedKeySerde).anyTimes();
    EasyMock.expect(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)).andReturn(false);
    EasyMock.expect(config.getKsqlStreamConfigProps()).andReturn(Collections.emptyMap());
    EasyMock.expect(materializedFactory.create(anyObject(), anyObject(), anyObject()))
        .andStubDelegateTo(new KsqlMaterializedFactory(config));
    EasyMock.replay(windowStartFunc, windowEndFunc, otherFunc, windowExp, config);
  }

  @Test
  public void shouldNoUseSelectMapperForNonWindowed() {
    // Given:
    final Map<Integer, KsqlAggregateFunction> invalidWindowFuncs = ImmutableMap.of(
        2, windowStartFunc, 4, windowEndFunc);

    // When:
    assertDoesNotInstallWindowSelectMapper(null, invalidWindowFuncs);
  }

  @Test
  public void shouldNotUseSelectMapperForWindowedWithoutWindowSelects() {
    // Given:
    final Map<Integer, KsqlAggregateFunction> nonWindowFuncs = ImmutableMap.of(2, otherFunc);

    // When:
    assertDoesNotInstallWindowSelectMapper(windowExp, nonWindowFuncs);
  }

  @Test
  public void shouldUseSelectMapperForWindowedWithWindowStart() {
    // Given:
    Map<Integer, KsqlAggregateFunction> funcMapWithWindowStart = ImmutableMap.of(
        0, otherFunc, 1, windowStartFunc);

    // Then:
    assertDoesInstallWindowSelectMapper(funcMapWithWindowStart);
  }

  @Test
  public void shouldUseSelectMapperForWindowedWithWindowEnd() {
    // Given:
    Map<Integer, KsqlAggregateFunction> funcMapWithWindowEnd = ImmutableMap.of(
        0, windowEndFunc, 1, otherFunc);

    // Then:
    assertDoesInstallWindowSelectMapper(funcMapWithWindowEnd);
  }

  @Test
  public void shouldUseStringKeySerdeForNoneWindowed() {
    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(
            initializer,emptyMap(), emptyMap(), null, topicValueSerDe, "GROUP");

    // Then:
    assertThat(result.getKeySerde(), instanceOf(Serdes.String().getClass()));
  }

  @Test
  public void shouldUseWindowExpressionKeySerde() {
    // Given:
    replay(ksqlWindowExp);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKeySerde(), is(sameInstance(windowedKeySerde)));
  }

  @Test
  public void shouldUseTimeWindowKeySerdeForWindowedIfLegacyConfig() {
    // Given:
    EasyMock.reset(config);
    EasyMock.expect(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG))
        .andReturn(true);
    EasyMock.expect(config.getKsqlStreamConfigProps()).andReturn(Collections.emptyMap());

    EasyMock.replay(config);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, "GROUP");

    // Then:
    assertThat(result.getKeySerde(),
        is(instanceOf(WindowedSerdes.timeWindowedSerdeFrom(String.class).getClass())));
  }

  private void assertDoesNotInstallWindowSelectMapper(
      final WindowExpression windowExp,
      final Map<Integer, KsqlAggregateFunction> funcMap) {

    // Given:
    if (windowExp != null) {
      EasyMock
          .expect(ksqlWindowExp.applyAggregate(anyObject(), anyObject(), anyObject(), anyObject()))
          .andReturn(table);
    } else {
      EasyMock
          .expect(groupedStream.aggregate(anyObject(), anyObject(), anyObject()))
          .andReturn(table);
    }

    EasyMock.expect(table.mapValues(anyObject(ValueMapper.class)))
        .andThrow(new AssertionError("Should not be called"))
        .anyTimes();
    EasyMock.expect(table.mapValues(anyObject(ValueMapperWithKey.class)))
        .andThrow(new AssertionError("Should not be called"))
        .anyTimes();

    EasyMock.replay(ksqlWindowExp, groupedStream, table);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table)));
    verify(table);
  }

  private void assertDoesInstallWindowSelectMapper(
      final Map<Integer, KsqlAggregateFunction> funcMap) {
    // Given:
    EasyMock
        .expect(ksqlWindowExp.applyAggregate(anyObject(), anyObject(), anyObject(), anyObject()))
        .andReturn(table);

    EasyMock.expect(table.mapValues(anyObject(ValueMapperWithKey.class)))
        .andReturn(table2)
        .once();

    EasyMock.replay(ksqlWindowExp, table);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table2)));
    verify(table);
  }

  @SuppressWarnings("unchecked")
  private Materialized expectMaterializedFactoryWithOpName(final String opName) {
    reset(materializedFactory);
    final Materialized materialized = EasyMock.mock(Materialized.class);
    expect(
        materializedFactory.create(
            anyObject(Serdes.String().getClass()), same(topicValueSerDe), eq(opName)))
        .andReturn(materialized);
    expect(materialized.withKeySerde(anyObject()))
        .andReturn(materialized);
    expect(materialized.withValueSerde(anyObject()))
        .andReturn(materialized);
    replay(materialized);
    return materialized;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseMaterializedFactoryForStateStore() {
    // Given:
    final Materialized materialized = expectMaterializedFactoryWithOpName(AGGREGATE_OP_NAME);
    final KTable mockKTable = mock(KTable.class);
    expect(groupedStream.aggregate(anyObject(), anyObject(), same(materialized)))
        .andReturn(mockKTable);
    replay(materializedFactory, groupedStream);

    // When:
    schemaGroupedStream.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        topicValueSerDe,
        AGGREGATE_OP_NAME
    );

    // Then:
    verify(groupedStream, materializedFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseMaterializedFactoryWindowedStateStore() {
    // Given:
    reset(ksqlWindowExp);
    final Materialized materialized = expectMaterializedFactoryWithOpName(AGGREGATE_OP_NAME);
    final KTable mockKTable = mock(KTable.class);
    final TimeWindowedKStream windowedKStream = mock(TimeWindowedKStream.class);
    expect(ksqlWindowExp.getKeySerde(String.class)).andReturn(windowedKeySerde).anyTimes();
    expect(ksqlWindowExp.applyAggregate(anyObject(), anyObject(), anyObject(), same(materialized)))
        .andReturn(table);
    expect(windowedKStream.aggregate(anyObject(), anyObject(), same(materialized)))
        .andReturn(mockKTable);
    replay(materializedFactory, groupedStream, windowedKStream, ksqlWindowExp);

    // When:
    schemaGroupedStream.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        windowExp,
        topicValueSerDe,
        AGGREGATE_OP_NAME);

    // Then:
    verify(groupedStream, materializedFactory);
  }
}
