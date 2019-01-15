/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.metastore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class ReadonlyMetaStoreTest {

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(ReadonlyMetaStore.class)
          .ignore("getTopic", String.class)
          .ignore("getSource", String.class)
          .ignore("getSourceForTopic", String.class)
          .ignore("getAllStructuredDataSources")
          .ignore("getAllKsqlTopics")
          .ignore("getQueriesWithSource", String.class)
          .ignore("getQueriesWithSink", String.class)
          .ignore("getUdfFactory", String.class)
          .ignore("isAggregate", String.class)
          .ignore("getAggregate", String.class, Schema.class)
          .ignore("copy")
          .ignore("listFunctions")
          .ignore("getAggregateFactory", String.class)
          .ignore("listAggregateFunctions")
          .build();
    }

    private final TestCase<ReadonlyMetaStore> testCase;
    private ReadonlyMetaStore readonlyMetaStore;

    public UnsupportedMethods(final TestCase<ReadonlyMetaStore> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      readonlyMetaStore = ReadonlyMetaStore.readOnlyMetaStore(mock(MetaStore.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(readonlyMetaStore);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private MetaStore delegate;
    @Mock
    private KsqlTopic ksqlTopic;
    @Mock
    private StructuredDataSource source;
    @Mock
    private UdfFactory function;
    @Mock
    private KsqlFunction ksqlFunction;
    @Mock
    private UdfFactory udfFactory;
    @Mock
    private Schema argumentType;
    @Mock
    private KsqlAggregateFunction ksqlAggFunc;
    @Mock
    private AggregateFunctionFactory aggFuncFactory;
    @Mock
    private MetaStore copy;
    private ReadonlyMetaStore readonlyMetaStore;

    @Before
    public void setUp() {
      readonlyMetaStore = ReadonlyMetaStore.readOnlyMetaStore(delegate);
    }

    @Test
    public void shouldGetTopic() {
      // Given:
      when(delegate.getTopic("some-topic")).thenReturn(ksqlTopic);

      // When:
      final KsqlTopic actual = readonlyMetaStore.getTopic("some-topic");

      // Then:
      assertThat(actual, is(sameInstance(ksqlTopic)));
    }

    @Test
    public void shouldGetSource() {
      // Given:
      when(delegate.getSource("some-source")).thenReturn(source);

      // When:
      final StructuredDataSource actual = readonlyMetaStore.getSource("some-source");

      // Then:
      assertThat(actual, is(sameInstance(source)));
    }

    @Test
    public void shouldGetSourceForTopic() {
      // Given:
      final Optional<StructuredDataSource> expected = Optional.of(source);
      when(delegate.getSourceForTopic("some-kafka-topic")).thenReturn(expected);

      // When:
      final Optional<StructuredDataSource> actual = readonlyMetaStore
          .getSourceForTopic("some-kafka-topic");

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetAllStructuredDataSources() {
      // Given:
      final Map<String, StructuredDataSource> expected = ImmutableMap.of("some-source", source);
      when(delegate.getAllStructuredDataSources()).thenReturn(expected);

      // When:
      final Map<String, StructuredDataSource> actual = readonlyMetaStore
          .getAllStructuredDataSources();

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetAllKsqlTopics() {
      // Given:
      final Map<String, KsqlTopic> expected = ImmutableMap.of("some-topic", ksqlTopic);
      when(delegate.getAllKsqlTopics()).thenReturn(expected);

      // When:
      final Map<String, KsqlTopic> actual = readonlyMetaStore.getAllKsqlTopics();

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetQueriesWithSource() {
      // Given:
      final Set<String> expected = ImmutableSet.of("source-query-id");
      when(delegate.getQueriesWithSource("source-name")).thenReturn(expected);

      // When:
      final Set<String> actual = readonlyMetaStore.getQueriesWithSource("source-name");

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetQueriesWithSink() {
      // Given:
      final Set<String> expected = ImmutableSet.of("sink-query-id");
      when(delegate.getQueriesWithSink("source-name")).thenReturn(expected);

      // When:
      final Set<String> actual = readonlyMetaStore.getQueriesWithSink("source-name");

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetUdfFactory() {
      // Given:
      when(delegate.getUdfFactory("source-function")).thenReturn(function);

      // When:
      final UdfFactory actual = readonlyMetaStore.getUdfFactory("source-function");

      // Then:
      assertThat(actual, is(sameInstance(function)));
    }

    @Test
    public void shouldGetIsAggregate() {
      // Given:
      when(delegate.isAggregate("source-function")).thenReturn(true);
      when(delegate.isAggregate("source-other-function")).thenReturn(false);

      // Then:
      assertThat(readonlyMetaStore.isAggregate("source-function"), is(true));
      assertThat(readonlyMetaStore.isAggregate("source-other-function"), is(false));
    }

    @Test
    public void shouldGetAggregate() {
      // Given:
      when(delegate.getAggregate("source-function", argumentType)).thenReturn(ksqlAggFunc);

      // When:
      final KsqlAggregateFunction actual = readonlyMetaStore
          .getAggregate("source-function", argumentType);

      // Then:
      assertThat(actual, is(sameInstance(ksqlAggFunc)));
    }

    @Test
    public void shouldCopy() {
      // Given:
      when(delegate.copy()).thenReturn(copy);

      // When:
      final MetaStore actual = readonlyMetaStore.copy();

      // Then:
      assertThat(actual, is(sameInstance(copy)));
    }

    @Test
    public void shouldListFunctions() {
      // Given:
      final List<UdfFactory> expected = ImmutableList.of(udfFactory);
      when(delegate.listFunctions()).thenReturn(expected);

      // When:
      final List<UdfFactory> actual = readonlyMetaStore.listFunctions();

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }

    @Test
    public void shouldGetAggregateFactory() {
      // Given:
      when(delegate.getAggregateFactory("some-function")).thenReturn(aggFuncFactory);

      // When:
      final AggregateFunctionFactory actual = readonlyMetaStore
          .getAggregateFactory("some-function");

      // Then:
      assertThat(actual, is(sameInstance(aggFuncFactory)));
    }

    @Test
    public void shouldListAggregateFunctions() {
      // Given:
      final List<AggregateFunctionFactory> expected = ImmutableList.of(aggFuncFactory);
      when(delegate.listAggregateFunctions()).thenReturn(expected);

      // When:
      final List<AggregateFunctionFactory> actual = readonlyMetaStore.listAggregateFunctions();

      // Then:
      assertThat(actual, is(sameInstance(expected)));
    }
  }
}