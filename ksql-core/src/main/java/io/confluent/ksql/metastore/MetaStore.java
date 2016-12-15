package io.confluent.ksql.metastore;

import java.util.Map;

public interface MetaStore {

  public KafkaTopic getTopic(String topicName);

  public void putTopic(KafkaTopic topic);

  public StructuredDataSource getSource(String sourceName);

  public void putSource(StructuredDataSource dataSource);

  public void deleteSource(String sourceName);

  public Map<String, StructuredDataSource> getAllStructuredDataSource();

  public Map<String, KafkaTopic> getAllKafkaTopics();
}
