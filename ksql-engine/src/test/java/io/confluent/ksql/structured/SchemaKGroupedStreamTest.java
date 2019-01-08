/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("unchecked")
public class SchemaKGroupedStreamTest {
  private static final String AGGREGATE_OP_NAME = "AGGREGATE";

  @Mock
  private Schema schema;
  @Mock
  private KGroupedStream groupedStream;
  @Mock
  private Field keyField;
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
  private Serde<Windowed<String>> windowedKeySerde;
  @Mock
  private MaterializedFactory materializedFactory;
  @Mock
  private Materialized materialized;
  private SchemaKGroupedStream schemaGroupedStream;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        schema, groupedStream, keyField, sourceStreams, config, funcRegistry, materializedFactory);

    when(windowStartFunc.getFunctionName()).thenReturn("WindowStart");
    when(windowEndFunc.getFunctionName()).thenReturn("WindowEnd");
    when(otherFunc.getFunctionName()).thenReturn("NotWindowStartFunc");
    when(windowExp.getKsqlWindowExpression()).thenReturn(ksqlWindowExp);
    when(ksqlWindowExp.getKeySerde(String.class)).thenReturn(windowedKeySerde);
    when(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG)).thenReturn(false);
    when(config.getKsqlStreamConfigProps()).thenReturn(Collections.emptyMap());
    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);

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
    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, emptyMap(), emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKeySerde(), is(sameInstance(windowedKeySerde)));
  }

  @Test
  public void shouldUseTimeWindowKeySerdeForWindowedIfLegacyConfig() {
    // Given:
    when(config.getBoolean(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG))
        .thenReturn(true);
    when(config.getKsqlStreamConfigProps()).thenReturn(Collections.emptyMap());


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
      when(ksqlWindowExp.applyAggregate(any(), any(), any(), any()))
          .thenReturn(table);
    } else {
      when(groupedStream.aggregate(any(), any(), any()))
          .thenReturn(table);
    }

    when(table.mapValues(any(ValueMapper.class)))
        .thenThrow(new AssertionError("Should not be called"));
    when(table.mapValues(any(ValueMapperWithKey.class)))
        .thenThrow(new AssertionError("Should not be called"));

    // When:
    final SchemaKTable result = schemaGroupedStream
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table)));
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
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe, "AGG");

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table2)));
    verify(table, times(1)).mapValues(any(ValueMapperWithKey.class));
  }

  @SuppressWarnings("unchecked")
  private Materialized whenMaterializedFactoryCreates() {
    final Materialized materialized = mock(Materialized.class);
    when(materializedFactory.create(any(), any(), any())).thenReturn(materialized);
    when(materialized.withKeySerde(any()))
        .thenReturn(materialized);
    when(materialized.withValueSerde(any()))
        .thenReturn(materialized);
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
        AGGREGATE_OP_NAME
    );

    // Then:
    verify(materializedFactory)
        .create(
            any(Serdes.String().getClass()),
            same(topicValueSerDe),
            eq(AGGREGATE_OP_NAME));
    verify(groupedStream, times(1)).aggregate(any(), any(), same(materialized));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUseMaterializedFactoryWindowedStateStore() {
    // Given:
    final Materialized materialized = whenMaterializedFactoryCreates();
    when(ksqlWindowExp.getKeySerde(String.class)).thenReturn(windowedKeySerde);
    when(ksqlWindowExp.applyAggregate(any(), any(), any(), same(materialized)))
        .thenReturn(table);

    // When:
    schemaGroupedStream.aggregate(
        () -> null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        windowExp,
        topicValueSerDe,
        AGGREGATE_OP_NAME);

    // Then:
    verify(materializedFactory)
        .create(
            any(Serdes.String().getClass()),
            same(topicValueSerDe),
            eq(AGGREGATE_OP_NAME));
    verify(ksqlWindowExp, times(1)).applyAggregate(any(), any(), any(), same(materialized));
  }
}
