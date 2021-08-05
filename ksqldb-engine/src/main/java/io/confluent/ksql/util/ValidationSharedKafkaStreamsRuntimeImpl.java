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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationSharedKafkaStreamsRuntimeImpl implements SharedKafkaStreamsRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(SharedKafkaStreamsRuntimeImpl.class);

  private KafkaStreamsNamedTopologyWrapper kafkaStreams;
  private ImmutableMap<String, Object> streamsProperties;
  private final Map<String, PersistentQueriesInSharedRuntimesImpl> metadata;
  public final Map<QueryId, Set<SourceName>> sources;

  public ValidationSharedKafkaStreamsRuntimeImpl(final KafkaStreamsBuilder kafkaStreamsBuilder,
                                                 final int maxQueryErrorsQueueSize,
                                                 final Map<String, Object> streamsProperties) {
    kafkaStreams = kafkaStreamsBuilder.build(streamsProperties);
    this.streamsProperties = ImmutableMap.copyOf(streamsProperties);
    metadata = new ConcurrentHashMap<>();
    sources = new ConcurrentHashMap<>();
  }

  public void markSources(final QueryId queryId, final Set<SourceName> sourceNames) {
    sources.put(queryId, sourceNames);
  }

  public void addQuery(
      final QueryErrorClassifier errorClassifier,
      final Map<String, Object> streamsProperties,
      final PersistentQueriesInSharedRuntimesImpl persistentQueriesInSharedRuntimesImpl,
      final QueryId queryId) {
    metadata.put(queryId.toString(), persistentQueriesInSharedRuntimesImpl);
    LOG.debug("mapping {}", metadata);
  }

  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable e
  ) {
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }

  public KafkaStreams.State state() {
    return kafkaStreams.state();
  }

  public Collection<StreamsMetadata> allMetadata() {
    return kafkaStreams.metadataForAllStreamsClients();
  }

  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return kafkaStreams.metadataForLocalThreads()
        .stream()
        .flatMap(t -> t.activeTasks().stream())
        .map(StreamsTaskMetadata::fromStreamsTaskMetadata)
        .collect(Collectors.toSet());
  }

  public void restart(final QueryId queryId) {
  }

  public boolean isError(final QueryId queryId) {
    return false;
  }

  public void close(final QueryId queryId) {

  }

  public synchronized void close() {
    for (String query: metadata.keySet()) {
      metadata.remove(query);
    }
    kafkaStreams.close();
  }

  public void start(final QueryId queryId) {
    if (metadata.containsKey(queryId.toString()) && !metadata.get(queryId.toString()).everStarted) {
      //don't start streams
    } else {
      throw new IllegalArgumentException("query not added to runtime");
    }
  }

  public List<QueryError> getQueryErrors() {
    return Collections.EMPTY_LIST;
  }

  public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(final QueryId queryId) {
    try {
      return kafkaStreams.allLocalStorePartitionLags();
    } catch (IllegalStateException | StreamsException e) {
      LOG.error(e.getMessage());
      return ImmutableMap.of();
    }
  }

  public Map<String, Object> getStreamProperties() {
    return streamsProperties;
  }

  public Set<SourceName> getSources() {
    return ImmutableSet.copyOf(
        sources
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toSet()));
  }

  public Set<QueryId> getQueries() {
    return ImmutableSet.copyOf(sources.keySet());
  }
}
