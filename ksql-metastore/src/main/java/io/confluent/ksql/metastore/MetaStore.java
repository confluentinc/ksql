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

import io.confluent.ksql.function.FunctionRegistry;
import java.util.Map;
import java.util.Set;

public interface MetaStore extends FunctionRegistry {

  KsqlTopic getTopic(String topicName);

  void putTopic(KsqlTopic topic);

  StructuredDataSource getSource(String sourceName);

  void putSource(StructuredDataSource dataSource);

  void deleteTopic(String topicName);

  void deleteSource(String sourceName);

  Map<String, StructuredDataSource> getAllStructuredDataSources();

  Set<String> getAllStructuredDataSourceNames();

  Map<String, KsqlTopic> getAllKsqlTopics();

  Set<String> getAllTopicNames();

  void updateForPersistentQuery(String queryId,
                                       Set<String> sourceNames,
                                       Set<String> sinkNames);

  void removePersistentQuery(String queryId);

  Set<String> getQueriesWithSource(String sourceName);

  Set<String> getQueriesWithSink(String sourceName);

  MetaStore clone();

}
