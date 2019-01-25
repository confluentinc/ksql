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

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;

public final class ReadonlyMetaStore implements MetaStore {

  private final MetaStore delegate;

  public static ReadonlyMetaStore readOnlyMetaStore(final MetaStore delegate) {
    return new ReadonlyMetaStore(delegate);
  }

  private ReadonlyMetaStore(final MetaStore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public KsqlTopic getTopic(final String topicName) {
    return delegate.getTopic(topicName);
  }

  @Override
  public void putTopic(final KsqlTopic topic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StructuredDataSource getSource(final String sourceName) {
    return delegate.getSource(sourceName);
  }

  @Override
  public Optional<StructuredDataSource> getSourceForTopic(final String ksqlTopicName) {
    return delegate.getSourceForTopic(ksqlTopicName);
  }

  @Override
  public void putSource(final StructuredDataSource dataSource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTopic(final String topicName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSource(final String sourceName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, StructuredDataSource> getAllStructuredDataSources() {
    return delegate.getAllStructuredDataSources();
  }

  @Override
  public Map<String, KsqlTopic> getAllKsqlTopics() {
    return delegate.getAllKsqlTopics();
  }

  @Override
  public void updateForPersistentQuery(
      final String queryId,
      final Set<String> sourceNames,
      final Set<String> sinkNames
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removePersistentQuery(final String queryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getQueriesWithSource(final String sourceName) {
    return delegate.getQueriesWithSource(sourceName);
  }

  @Override
  public Set<String> getQueriesWithSink(final String sourceName) {
    return delegate.getQueriesWithSink(sourceName);
  }

  @Override
  public UdfFactory getUdfFactory(final String functionName) {
    return delegate.getUdfFactory(functionName);
  }

  @Override
  public void addFunction(final KsqlFunction ksqlFunction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addFunctionFactory(final UdfFactory factory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAggregate(final String functionName) {
    return delegate.isAggregate(functionName);
  }

  @Override
  public KsqlAggregateFunction getAggregate(final String functionName, final Schema argumentType) {
    return delegate.getAggregate(functionName, argumentType);
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaStore copy() {
    return delegate.copy();
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return delegate.listFunctions();
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    return delegate.getAggregateFactory(functionName);
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return delegate.listAggregateFunctions();
  }
}
