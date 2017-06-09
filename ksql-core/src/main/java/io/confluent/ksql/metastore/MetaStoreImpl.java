/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KsqlException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetaStoreImpl implements MetaStore {

  Map<String, KsqlTopic> topicMap = new HashMap<>();

  Map<String, StructuredDataSource> dataSourceMap = new HashMap<>();

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
          + dataSource.getName());
    }
  }

  @Override
  public void deleteSource(final String sourceName) {
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
}
