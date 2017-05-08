/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import java.util.Map;
import java.util.Set;

public interface MetaStore {

  public KSQLTopic getTopic(String topicName);

  public void putTopic(KSQLTopic topic);

  public StructuredDataSource getSource(String sourceName);

  public void putSource(StructuredDataSource dataSource);

  public void deleteSource(String sourceName);

  public Map<String, StructuredDataSource> getAllStructuredDataSources();

  public Set<String> getAllStructuredDataSourceNames();

  public Map<String, KSQLTopic> getAllKQLTopics();

  public Set<String> getAllTopicNames();
}
