package io.confluent.kql.metastore;

import java.util.Map;

public interface MetaStore {

  public KQLTopic getTopic(String topicName);

  public void putTopic(KQLTopic topic);

  public StructuredDataSource getSource(String sourceName);

  public void putSource(StructuredDataSource dataSource);

  public void deleteSource(String sourceName);

  public Map<String, StructuredDataSource> getAllStructuredDataSource();

  public Map<String, KQLTopic> getAllKafkaTopics();
}
