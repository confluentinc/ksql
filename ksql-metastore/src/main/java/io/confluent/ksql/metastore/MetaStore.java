/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metastore;

import io.confluent.ksql.function.FunctionRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface MetaStore extends FunctionRegistry {

  KsqlTopic getTopic(String topicName);

  StructuredDataSource getSource(String sourceName);

  Optional<StructuredDataSource> getSourceForTopic(String ksqlTopicName);

  Map<String, StructuredDataSource> getAllStructuredDataSources();

  Map<String, KsqlTopic> getAllKsqlTopics();

  Set<String> getQueriesWithSource(String sourceName);

  Set<String> getQueriesWithSink(String sourceName);

  MetaStore copy();
}
