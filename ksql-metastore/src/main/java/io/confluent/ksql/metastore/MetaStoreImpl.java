/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.Pair;

public class MetaStoreImpl implements MetaStore, Cloneable {

  private final Map<String, KsqlTopic> topicMap;
  private final Map<String,
      Pair<StructuredDataSource, ReferentialIntegrityTableEntry>> dataSourceMap;
  private final FunctionRegistry functionRegistry;

  public MetaStoreImpl(final FunctionRegistry functionRegistry) {
    this(new HashMap<>(), new HashMap<>(), functionRegistry);
  }

  private MetaStoreImpl(
      final Map<String, KsqlTopic> topicMap,
      final Map<String, Pair<StructuredDataSource, ReferentialIntegrityTableEntry>> dataSourceMap,
      final FunctionRegistry functionRegistry
  ) {
    this.topicMap = (topicMap != null) ? topicMap : new HashMap<>();
    this.dataSourceMap = (dataSourceMap != null) ? dataSourceMap : new HashMap<>();
    this.functionRegistry = functionRegistry;
  }

  @Override
  public KsqlTopic getTopic(String topicName) {
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
    if (!dataSourceMap.containsKey(sourceName)) {
      return null;
    }
    return dataSourceMap.get(sourceName).getLeft();
  }

  @Override
  public void putSource(final StructuredDataSource dataSource) {

    if (getSource(dataSource.getName()) == null) {
      dataSourceMap.put(dataSource.getName(), new Pair<>(dataSource, new
          ReferentialIntegrityTableEntry()));
    } else {
      throw new KsqlException(
          "Cannot add the new data source. Another data source with the same name already exists: "
          + dataSource.toString());
    }
  }

  @Override
  public void deleteTopic(String topicName) {
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
      String sourceForQueriesMessage = dataSourceMap.get(sourceName).getRight()
          .getSourceForQueries()
          .stream()
          .collect(Collectors.joining(", "));
      String sinkForQueriesMessage = dataSourceMap.get(sourceName).getRight()
          .getSinkForQueries()
          .stream()
          .collect(Collectors.joining(", "));
      throw new KsqlReferentialIntegrityException(
          String.format("Cannot drop %s. \nThe following queries "
                        + "read from this source: [%s]. \nThe following "
                        + "queries write into this source: [%s]. \nYou need to "
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
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getLeft()));
  }

  @Override
  public Set<String> getAllStructuredDataSourceNames() {
    return getAllStructuredDataSources().keySet();
  }

  @Override
  public Map<String, KsqlTopic> getAllKsqlTopics() {
    return topicMap;
  }

  @Override
  public Set<String> getAllTopicNames() {
    return getAllKsqlTopics().keySet();
  }

  @Override
  public void updateForPersistentQuery(final String queryId,
                                       final Set<String> sourceNames,
                                       final Set<String> sinkNames) {
    addSourceNames(sourceNames, queryId);
    addSinkNames(sinkNames, queryId);

  }

  private void addSourceNames(final Set<String> sourceNames, final String queryId) {
    for (String sourceName: sourceNames) {
      ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
          dataSourceMap.get(sourceName).getRight();
      referentialIntegrityTableEntry.getSourceForQueries().add(queryId);
    }
  }

  private void addSinkNames(final Set<String> sinkNames, final String queryId) {
    for (String sinkName: sinkNames) {
      ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
          dataSourceMap.get(sinkName).getRight();
      referentialIntegrityTableEntry.getSinkForQueries().add(queryId);
    }
  }

  @Override
  public void removePersistentQuery(final String queryId) {
    for (Pair<StructuredDataSource, ReferentialIntegrityTableEntry>
        structuredDataSourceReferentialIntegrityTableEntryPair: dataSourceMap.values()) {
      structuredDataSourceReferentialIntegrityTableEntryPair.getRight()
          .removeQuery(queryId);
    }
  }

  private boolean isSafeToDrop(final String sourceName) {
    if (!dataSourceMap.containsKey(sourceName)) {
      return true;
    }
    ReferentialIntegrityTableEntry referentialIntegrityTableEntry =
        dataSourceMap.get(sourceName).getRight();
    return (referentialIntegrityTableEntry.getSinkForQueries().isEmpty()
            && referentialIntegrityTableEntry.getSourceForQueries().isEmpty());
  }

  @Override
  public Set<String> getQueriesWithSource(final String sourceName) {
    return Collections.unmodifiableSet(dataSourceMap.get(sourceName).getRight()
                                           .getSourceForQueries());
  }

  @Override
  public Set<String> getQueriesWithSink(final String sourceName) {
    return Collections.unmodifiableSet(dataSourceMap.get(sourceName).getRight()
                                           .getSinkForQueries());
  }

  @Override
  public MetaStore clone() {
    Map<String, KsqlTopic> cloneTopicMap = new HashMap<>(topicMap);
    Map<String, Pair<StructuredDataSource, ReferentialIntegrityTableEntry>> cloneDataSourceMap =
        dataSourceMap
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> new Pair<>(entry.getValue().getLeft(), entry.getValue().getRight().clone())));
    cloneDataSourceMap.putAll(dataSourceMap);
    return new MetaStoreImpl(cloneTopicMap, cloneDataSourceMap, functionRegistry.copy());
  }

  @Override
  public UdfFactory getUdfFactory(String functionName) {
    return functionRegistry.getUdfFactory(functionName);
  }

  @Override
  public void addFunction(KsqlFunction ksqlFunction) {
    functionRegistry.addFunction(ksqlFunction);
  }

  @Override
  public boolean addFunctionFactory(final UdfFactory factory) {
    return functionRegistry.addFunctionFactory(factory);
  }

  public boolean isAggregate(String functionName) {
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
  public MetaStore copy() {
    return clone();
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
}
