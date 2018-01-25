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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.confluent.ksql.util.KsqlException;

public class MetaStoreImpl implements MetaStore, Cloneable {

  private final Map<String, KsqlTopic> topicMap;
  private final Map<String, StructuredDataSource> dataSourceMap;

  public MetaStoreImpl() {
    this.topicMap = new HashMap<>();
    this.dataSourceMap = new HashMap<>();
  }

  private MetaStoreImpl(
      Map<String, KsqlTopic> topicMap,
      Map<String, StructuredDataSource> dataSourceMap
  ) {
    this.topicMap = (topicMap != null) ? topicMap : new HashMap<>();
    this.dataSourceMap = (dataSourceMap != null) ? dataSourceMap : new HashMap<>();
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
    return dataSourceMap.get(sourceName);
  }

  @Override
  public void putSource(final StructuredDataSource dataSource) {

    if (getSource(dataSource.getName()) == null) {
      dataSourceMap.put(dataSource.getName(), dataSource);
    } else {
      throw new KsqlException(
          "Cannot add the new data source. Another data source with the same name already exists: "
          + dataSource.toString());
    }
  }

  @Override
  public void deleteTopic(String topicName) {
    if (!topicMap.containsKey(topicName)) {
      throw new KsqlException(String.format("No topic with name %s was registered.", true));
    }
    topicMap.remove(topicName);
  }

  @Override
  public void deleteSource(final String sourceName) {
    if (!dataSourceMap.containsKey(sourceName)) {
      throw new KsqlException(String.format("No data source with name %s exists.", sourceName));
    }
    dataSourceMap.remove(sourceName);
  }

  @Override
  public Map<String, StructuredDataSource> getAllStructuredDataSources() {
    return dataSourceMap;
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
  public void putAll(MetaStore otherMetaStore) {
    this.topicMap.putAll(otherMetaStore.getAllKsqlTopics());
    this.dataSourceMap.putAll(otherMetaStore.getAllStructuredDataSources());
  }

  @Override
  public MetaStore clone() {
    Map<String, KsqlTopic> cloneTopicMap = new HashMap<>();
    Map<String, StructuredDataSource> cloneDataSourceMap = new HashMap<>();

    cloneTopicMap.putAll(topicMap);
    cloneDataSourceMap.putAll(dataSourceMap);

    return new MetaStoreImpl(cloneTopicMap, cloneDataSourceMap);
  }

}
