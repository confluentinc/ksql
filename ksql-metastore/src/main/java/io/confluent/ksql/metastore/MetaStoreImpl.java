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

package io.confluent.ksql.metastore;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public final class MetaStoreImpl implements MetaStore {

  private final Map<String, KsqlTopic> topicMap = new HashMap<>();
  private final Map<String, SourceInfo> dataSourceMap = new HashMap<>();
  private final FunctionRegistry functionRegistry;

  public MetaStoreImpl(final FunctionRegistry functionRegistry) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  private MetaStoreImpl(
      final Map<String, KsqlTopic> topicMap,
      final Map<String, SourceInfo> dataSourceMap,
      final FunctionRegistry functionRegistry
  ) {
    this.topicMap.putAll(topicMap);
    this.functionRegistry = functionRegistry.copy();

    dataSourceMap.forEach((name, info) -> this.dataSourceMap.put(name, info.copy()));
  }

  @Override
  public KsqlTopic getTopic(final String topicName) {
    return topicMap.get(topicName);
  }

  @Override
  public void putTopic(final KsqlTopic topic) {
    if (topicMap.get(topic.getName()) == null) {
      topicMap.put(topic.getName(), topic);
    } else {
      throw new KsqlException(
          "Cannot add the new topic. Another topic with the same name already exists: "
          + topic.getName());
    }
  }

  @Override
  public StructuredDataSource getSource(final String sourceName) {
    final SourceInfo source = dataSourceMap.get(sourceName);
    if (source == null) {
      return null;
    }
    return source.source;
  }

  @Override
  public Optional<StructuredDataSource> getSourceForTopic(final String ksqlTopicName) {
    return dataSourceMap.values()
        .stream()
        .filter(p -> p.source.getKsqlTopic().getName() != null
            && p.source.getKsqlTopic().getName().equals(ksqlTopicName))
        .map(sourceInfo -> sourceInfo.source)
        .findFirst();
  }

  @Override
  public void putSource(final StructuredDataSource dataSource) {

    if (dataSourceMap.containsKey(dataSource.getName())) {
      throw new KsqlException(
          "Cannot add the new data source. Another data source with the same name already exists: "
              + dataSource.toString());
    }

    dataSourceMap.put(dataSource.getName(), new SourceInfo(dataSource));
  }

  @Override
  public void deleteTopic(final String topicName) {
    if (!topicMap.containsKey(topicName)) {
      throw new KsqlException(String.format("No topic with name %s was registered.", topicName));
    }
    topicMap.remove(topicName);
  }

  @Override
  public void deleteSource(final String sourceName) {
    if (!dataSourceMap.containsKey(sourceName)) {
      throw new KsqlException(String.format("No data source with name %s exists.", sourceName));
    }
    if (!isSafeToDrop(sourceName)) {
      final String sourceForQueriesMessage = dataSourceMap.get(sourceName).referentialIntegrity
          .getSourceForQueries()
          .stream()
          .collect(Collectors.joining(", "));
      final String sinkForQueriesMessage = dataSourceMap.get(sourceName).referentialIntegrity
          .getSinkForQueries()
          .stream()
          .collect(Collectors.joining(", "));
      throw new KsqlReferentialIntegrityException(
          String.format("Cannot drop %s. %nThe following queries "
                        + "read from this source: [%s]. %nThe following "
                        + "queries write into this source: [%s]. %nYou need to "
                        + "terminate them before dropping %s.",
                        sourceName, sourceForQueriesMessage, sinkForQueriesMessage, sourceName));
    }
    dataSourceMap.remove(sourceName);
  }

  @Override
  public Map<String, StructuredDataSource> getAllStructuredDataSources() {
    return dataSourceMap
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().source));
  }

  @Override
  public Map<String, KsqlTopic> getAllKsqlTopics() {
    return Collections.unmodifiableMap(topicMap);
  }

  @Override
  public void updateForPersistentQuery(final String queryId,
                                       final Set<String> sourceNames,
                                       final Set<String> sinkNames) {
    addSourceNames(sourceNames, queryId);
    addSinkNames(sinkNames, queryId);

  }

  private void addSourceNames(final Set<String> sourceNames, final String queryId) {
    for (final String sourceName: sourceNames) {
      final ReferentialIntegrityTableEntry referentialIntegrity =
          dataSourceMap.get(sourceName).referentialIntegrity;

      referentialIntegrity.addSourceForQueries(queryId);
    }
  }

  private void addSinkNames(final Set<String> sinkNames, final String queryId) {
    for (final String sinkName: sinkNames) {
      final ReferentialIntegrityTableEntry referentialIntegrity =
          dataSourceMap.get(sinkName).referentialIntegrity;

      referentialIntegrity.addSinkForQueries(queryId);
    }
  }

  @Override
  public void removePersistentQuery(final String queryId) {
    for (final SourceInfo sourceInfo : dataSourceMap.values()) {
      sourceInfo.referentialIntegrity.removeQuery(queryId);
    }
  }

  private boolean isSafeToDrop(final String sourceName) {
    final SourceInfo sourceInfo = dataSourceMap.get(sourceName);
    if (sourceInfo == null) {
      return true;
    }

    final ReferentialIntegrityTableEntry referentialIntegrity =
        sourceInfo.referentialIntegrity;

    return (referentialIntegrity.getSinkForQueries().isEmpty()
        && referentialIntegrity.getSourceForQueries().isEmpty());
  }

  @Override
  public Set<String> getQueriesWithSource(final String sourceName) {
    return dataSourceMap.get(sourceName).referentialIntegrity.getSourceForQueries();
  }

  @Override
  public Set<String> getQueriesWithSink(final String sourceName) {
    return dataSourceMap.get(sourceName).referentialIntegrity.getSinkForQueries();
  }

  @Override
  public MetaStore copy() {
    return new MetaStoreImpl(topicMap, dataSourceMap, functionRegistry);
  }

  @Override
  public UdfFactory getUdfFactory(final String functionName) {
    return functionRegistry.getUdfFactory(functionName);
  }

  @Override
  public void addFunction(final KsqlFunction ksqlFunction) {
    functionRegistry.addFunction(ksqlFunction);
  }

  @Override
  public boolean addFunctionFactory(final UdfFactory factory) {
    return functionRegistry.addFunctionFactory(factory);
  }

  public boolean isAggregate(final String functionName) {
    return functionRegistry.isAggregate(functionName);
  }

  public KsqlAggregateFunction getAggregate(final String functionName,
                                            final Schema argumentType) {
    return functionRegistry.getAggregate(functionName, argumentType);
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    functionRegistry.addAggregateFunctionFactory(aggregateFunctionFactory);
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return functionRegistry.listFunctions();
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    return functionRegistry.getAggregateFactory(functionName);
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return functionRegistry.listAggregateFunctions();
  }

  private static final class SourceInfo {

    private final StructuredDataSource source;
    private final ReferentialIntegrityTableEntry referentialIntegrity;

    private SourceInfo(
        final StructuredDataSource source
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = new ReferentialIntegrityTableEntry();
    }

    private SourceInfo(
        final StructuredDataSource source,
        final ReferentialIntegrityTableEntry referentialIntegrity
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = referentialIntegrity.copy();
    }

    public SourceInfo copy() {
      return new SourceInfo(source, referentialIntegrity);
    }
  }
}
