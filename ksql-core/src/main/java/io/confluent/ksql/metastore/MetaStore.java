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

import java.util.Map;
import java.util.Set;

public interface MetaStore {

  public KsqlTopic getTopic(String topicName);

  public void putTopic(KsqlTopic topic);

  public StructuredDataSource getSource(String sourceName);

  public void putSource(StructuredDataSource dataSource);

  public void deleteTopic(String topicName);

  public void deleteSource(String sourceName);

  public Map<String, StructuredDataSource> getAllStructuredDataSources();

  public Set<String> getAllStructuredDataSourceNames();

  public Map<String, KsqlTopic> getAllKsqlTopics();

  public Set<String> getAllTopicNames();

  public void putAll(MetaStore otherMetaStore);

  public MetaStore clone();
}
