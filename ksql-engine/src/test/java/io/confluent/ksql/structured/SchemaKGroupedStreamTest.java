/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedStreamTest {
  @Mock
  private LogicalSchema schema;
  @Mock
  private KGroupedStream groupedStream;
  @Mock
  private KeyField keyField;
  @Mock
  private List<SchemaKStream> sourceStreams;
  @Mock
  private KsqlConfig config;
  @Mock
  private FunctionRegistry funcRegistry;
  @Mock
  private Initializer initializer;
  @Mock
  private Serde<GenericRow> topicValueSerDe;
  @Mock
  private KsqlAggregateFunction windowStartFunc;
  @Mock
  private KsqlAggregateFunction windowEndFunc;
  @Mock
  private KsqlAggregateFunction otherFunc;
  @Mock
  private KTable table;
  @Mock
  private KTable table2;
  @Mock
  private WindowExpression windowExp;
  @Mock
  private KsqlWindowExpression ksqlWindowExp;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized materialized;
  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private KeySerde<Windowed<Struct>> windowedKeySerde;
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        groupedStream, schema, keySerde, keyField, sourceStreams, config, funcRegistry,
        materializedFactory
    );

    when(windowStartFunc.getFunctionName()).thenReturn("WindowStart");
    when(windowEndFunc.getFunctionName()).thenReturn("WindowEnd");
    when(otherFunc.getFunctionName()).thenReturn("NotWindowStartFunc");
    when(windowExp.getKsqlWindowExpression()).thenReturn(ksqlWindowExp);
    when(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)).thenReturn(false);
    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);

    when(ksqlWindowExp.getWindowInfo())
        .thenReturn(WindowInfo.of(WindowType.SESSION, Optional.empty()));

    when(keySerde.rebind(any(WindowInfo.class))).thenReturn(windowedKeySerde);
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
  public void shouldSupportSessionWindowedKey() {
    // Given:
    final WindowInfo windowInfo = WindowInfo.of(WindowType.SESSION, Optional.empty());
    when(ksqlWindowExp.getWindowInfo()).thenReturn(windowInfo);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    verify(keySerde).rebind(windowInfo);
    assertThat(result.getKeySerde(), is(windowedKeySerde));
  }

  @Test
  public void shouldSupportHoppingWindowedKey() {
    // Given:
    final WindowInfo windowInfo = WindowInfo
        .of(WindowType.HOPPING, Optional.of(Duration.ofMillis(10)));

    when(ksqlWindowExp.getWindowInfo()).thenReturn(windowInfo);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    verify(keySerde).rebind(windowInfo);
    assertThat(result.getKeySerde(), is(windowedKeySerde));
  }

  @Test
  public void shouldSupportTumblingWindowedKey() {
    // Given:
    final WindowInfo windowInfo = WindowInfo
        .of(WindowType.TUMBLING, Optional.of(Duration.ofMillis(10)));

    when(ksqlWindowExp.getWindowInfo()).thenReturn(windowInfo);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    verify(keySerde).rebind(windowInfo);
    assertThat(result.getKeySerde(), is(windowedKeySerde));
  }

  @Test
  public void shouldUseTimeWindowKeySerdeForWindowedIfLegacyConfig() {
    // Given:
    when(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG))
        .thenReturn(true);

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    verify(keySerde)
        .rebind(WindowInfo.of(WindowType.TUMBLING, Optional.of(Duration.ofMillis(Long.MAX_VALUE))));
    assertThat(result.getKeySerde(), is(windowedKeySerde));
  }

  private void assertDoesNotInstallWindowSelectMapper(
      final WindowExpression windowExp,
      final Map<Integer, KsqlAggregateFunction> funcMap) {

    // Given:
    if (windowExp != null) {
      when(ksqlWindowExp.applyAggregate(any(), any(), any(), any()))
          .thenReturn(table);
    } else {
      when(groupedStream.aggregate(any(), any(), any()))
          .thenReturn(table);
    }

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table)));
    verify(table, never()).mapValues(any(ValueMapper.class));
    verify(table, never()).mapValues(any(ValueMapperWithKey.class));
  }

  private void assertDoesInstallWindowSelectMapper(
      final Map<Integer, KsqlAggregateFunction> funcMap) {
    // Given:
    when(ksqlWindowExp.applyAggregate(any(), any(), any(), any()))
        .thenReturn(table);

    when(table.mapValues(any(ValueMapperWithKey.class)))
        .thenReturn(table2);


    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, queryContext);

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table2)));
    verify(table, times(1)).mapValues(any(ValueMapperWithKey.class));
  }

  @SuppressWarnings("unchecked")
  private Materialized whenMaterializedFactoryCreates() {
    final Materialized materialized = mock(Materialized.class);
    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);
    return materialized;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseMaterializedFactoryForStateStore() {
    // Given:
    final Materialized materialized = whenMaterializedFactoryCreates();
    final KTable mockKTable = mock(KTable.class);
    when(groupedStream.aggregate(any(), any(), same(materialized)))
        .thenReturn(mockKTable);

    // When:
    schemaGroupedStream.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        topicValueSerDe,
        queryContext
    );

    // Then:
    verify(materializedFactory)
        .create(
            same(keySerde),
            same(topicValueSerDe),
            eq(StreamsUtil.buildOpName(queryContext.getQueryContext())));
    verify(groupedStream, times(1)).aggregate(any(), any(), same(materialized));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseMaterializedFactoryWindowedStateStore() {
    // Given:
    final Materialized materialized = whenMaterializedFactoryCreates();
    when(ksqlWindowExp.applyAggregate(any(), any(), any(), same(materialized)))
        .thenReturn(table);

    // When:
    schemaGroupedStream.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        windowExp,
        topicValueSerDe,
        queryContext);

    // Then:
    verify(materializedFactory)
        .create(
            same(keySerde),
            same(topicValueSerDe),
            eq(StreamsUtil.buildOpName(queryContext.getQueryContext())));
    verify(ksqlWindowExp, times(1)).applyAggregate(any(), any(), any(), same(materialized));
  }
}
