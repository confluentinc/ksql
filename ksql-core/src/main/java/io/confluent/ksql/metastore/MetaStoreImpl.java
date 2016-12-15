package io.confluent.ksql.metastore;

import io.confluent.ksql.util.KSQLException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaStoreImpl implements MetaStore {

  Map<String, KafkaTopic> topicMap = new HashMap<>();

  Map<String, StructuredDataSource> dataSourceMap = new HashMap<>();

  @Override
  public KafkaTopic getTopic(String topicName) {
    return topicMap.get(topicName.toUpperCase());
  }

  @Override
  public void putTopic(KafkaTopic topic) {
    if (topicMap.get(topic.getName().toUpperCase()) == null) {
      topicMap.put(topic.getName().toUpperCase(), topic);
    } else {
      throw new KSQLException(
          "Cannot add the new topic. Another topic with the same name already exists: "
          + topic.getName());
    }
  }

  @Override
  public StructuredDataSource getSource(String sourceName) {
    return dataSourceMap.get(sourceName.toUpperCase());
  }

  @Override
  public void putSource(StructuredDataSource dataSource) {
    if (getSource(dataSource.getName()) == null) {
      dataSourceMap.put(dataSource.getName().toUpperCase(), dataSource);
    } else {
      throw new KSQLException(
          "Cannot add the new data source. Another data source with the same name already exists: "
          + dataSource.getName());
    }

  }

  @Override
  public void deleteSource(String sourceName) {
    dataSourceMap.remove(sourceName);
  }

  @Override
  public Map<String, StructuredDataSource> getAllStructuredDataSource() {
    return dataSourceMap;
  }

  @Override
  public Map<String, KafkaTopic> getAllKafkaTopics() {
    return topicMap;
  }
}
