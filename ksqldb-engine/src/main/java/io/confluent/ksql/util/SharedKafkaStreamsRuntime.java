/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;


public interface SharedKafkaStreamsRuntime {

  void markSources(QueryId queryId, Set<SourceName> sourceNames);

  void register(
      QueryErrorClassifier errorClassifier,
      Map<String, Object> streamsProperties,
      PersistentQueriesInSharedRuntimesImpl persistentQueriesInSharedRuntimesImpl,
      QueryId queryId);

  StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      Throwable e
  );

  KafkaStreams getKafkaStreams();

  KafkaStreams.State state();

  Collection<StreamsMetadata> allMetadata();

  Set<StreamsTaskMetadata> getTaskMetadata();

  boolean isError(QueryId queryId);

  void stop(QueryId queryId);

  void close();

  void start(QueryId queryId);

  List<QueryError> getQueryErrors();

  Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(QueryId queryId);

  Map<String, Object> getStreamProperties();

  Set<SourceName> getSources();

  Set<QueryId> getQueries();

  KafkaStreamsBuilder getKafkaStreamsBuilder();

  Map<String, PersistentQueriesInSharedRuntimesImpl> getMetadata();

  Map<QueryId, Set<SourceName>> getSourcesMap();

  void addQueryError(QueryError e);
}
