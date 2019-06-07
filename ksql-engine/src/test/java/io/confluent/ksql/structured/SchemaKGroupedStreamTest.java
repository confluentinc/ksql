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
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
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
  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        schema, groupedStream, keyField, sourceStreams, funcRegistry, srClient);

    EasyMock.expect(windowStartFunc.getFunctionName()).andReturn("WindowStart").anyTimes();
    EasyMock.expect(windowEndFunc.getFunctionName()).andReturn("WindowEnd").anyTimes();
    EasyMock.expect(otherFunc.getFunctionName()).andReturn("NotWindowStartFunc").anyTimes();
    EasyMock.expect(windowExp.getKsqlWindowExpression()).andReturn(ksqlWindowExp).anyTimes();
    EasyMock.replay(windowStartFunc, windowEndFunc, otherFunc, windowExp);
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
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe);

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
        .aggregate(initializer, funcMap, emptyMap(), windowExp, topicValueSerDe);

    // Then:
    assertThat(result.getKtable(), is(sameInstance(table2)));
    verify(table);
  }
}