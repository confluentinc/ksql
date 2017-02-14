/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import io.confluent.kql.util.KQLException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetaStoreImpl implements MetaStore {

  Map<String, KQLTopic> topicMap = new HashMap<>();

  Map<String, StructuredDataSource> dataSourceMap = new HashMap<>();

  @Override
  public KQLTopic getTopic(String topicName) {
    return topicMap.get(topicName.toUpperCase());
  }

  @Override
  public void putTopic(final KQLTopic topic) {
    if (topicMap.get(topic.getName().toUpperCase()) == null) {
      topicMap.put(topic.getName().toUpperCase(), topic);
    } else {
      throw new KQLException(
          "Cannot add the new topic. Another topic with the same name already exists: "
          + topic.getName());
    }
  }

  @Override
  public StructuredDataSource getSource(final String sourceName) {
    return dataSourceMap.get(sourceName.toUpperCase());
  }

  @Override
  public void putSource(final StructuredDataSource dataSource) {
    if (getSource(dataSource.getName()) == null) {
      dataSourceMap.put(dataSource.getName().toUpperCase(), dataSource);
    } else {
      throw new KQLException(
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
  public Map<String, KQLTopic> getAllKQLTopics() {
    return topicMap;
  }

  @Override
  public Set<String> getAllTopicNames() {
    return getAllKQLTopics().keySet();
  }
}
