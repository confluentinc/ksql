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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationSharedKafkaStreamsRuntimeImpl implements SharedKafkaStreamsRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(SharedKafkaStreamsRuntimeImpl.class);

  private final KafkaStreamsNamedTopologyWrapper kafkaStreams;
  private final Map<String, Object> streamsProperties;
  private final Map<String, PersistentQueriesInSharedRuntimesImpl> metadata;
  private final Map<QueryId, Set<SourceName>> sources;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;

  public ValidationSharedKafkaStreamsRuntimeImpl(
      final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime
  ) {
    streamsProperties = new ConcurrentHashMap<>(sharedKafkaStreamsRuntime.getStreamProperties());
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,
        "_confluent-ksql-" + UUID.randomUUID() + "-validation"
    );
    kafkaStreamsBuilder = sharedKafkaStreamsRuntime.getKafkaStreamsBuilder();
    kafkaStreams = kafkaStreamsBuilder.buildNamedTopologyWrapper(streamsProperties);
    metadata = new ConcurrentHashMap<>(sharedKafkaStreamsRuntime.getMetadata());
    sources = new ConcurrentHashMap<>(sharedKafkaStreamsRuntime.getSourcesMap());
    for (PersistentQueriesInSharedRuntimesImpl queryMetadata : metadata.values()) {
      //kafkaStreams.addNamedTopology(queryMetadata.getTopology());
    }
  }

  public ValidationSharedKafkaStreamsRuntimeImpl(final KafkaStreamsBuilder kafkaStreamsBuilder,
                                                 final int maxQueryErrorsQueueSize,
                                                 final Map<String, Object> streamsProperties) {
    kafkaStreams = kafkaStreamsBuilder.buildNamedTopologyWrapper(streamsProperties);
    this.streamsProperties = ImmutableMap.copyOf(streamsProperties);
    metadata = new ConcurrentHashMap<>();
    sources = new ConcurrentHashMap<>();
    this.kafkaStreamsBuilder = kafkaStreamsBuilder;
  }

  public void markSources(final QueryId queryId, final Set<SourceName> sourceNames) {
    sources.put(queryId, sourceNames);
  }

  public void register(
      final QueryErrorClassifier errorClassifier,
      final Map<String, Object> streamsProperties,
      final PersistentQueriesInSharedRuntimesImpl persistentQueriesInSharedRuntimesImpl,
      final QueryId queryId) {
    if (!sources.containsKey(queryId)) {
      if (sources
          .values()
          .stream()
          .flatMap(Collection::stream)
          .anyMatch(t -> persistentQueriesInSharedRuntimesImpl.getSourceNames().contains(t))) {
        throw new IllegalArgumentException(
            queryId.toString() + ": was not reserved on this runtime");
      } else {
        sources.put(queryId, persistentQueriesInSharedRuntimesImpl.getSourceNames());
      }
    }

    metadata.put(queryId.toString(), persistentQueriesInSharedRuntimesImpl);
    LOG.debug("mapping {}", metadata);
  }

  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable e
  ) {
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "must expose streams")
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

  public boolean isError(final QueryId queryId) {
    return false;
  }

  public void stop(final QueryId queryId) {

  }

  public synchronized void close() {
    kafkaStreams.close();
    kafkaStreams.cleanUp();
  }

  public void start(final QueryId queryId) {
    if (metadata.containsKey(queryId.toString()) && !metadata.get(queryId.toString()).everStarted) {
      if (!kafkaStreams.getTopologyByName(queryId.toString()).isPresent()) {
        //kafkaStreams.addNamedTopology(metadata.get(queryId.toString()).getTopology());
      } else {
        throw new IllegalArgumentException("not done removing query: " + queryId);
      }
    } else {
      throw new IllegalArgumentException("query: " + queryId + " not added to runtime");
    }
  }

  public List<QueryError> getQueryErrors() {
    return Collections.emptyList();
  }

  public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(final QueryId queryId) {
    try {
      return kafkaStreams.allLocalStorePartitionLags();
    } catch (IllegalStateException | StreamsException e) {
      LOG.error(e.getMessage());
      return ImmutableMap.of();
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streamsProperties is immutable")
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

  @Override
  public KafkaStreamsBuilder getKafkaStreamsBuilder() {
    return kafkaStreamsBuilder;
  }

  @Override
  public Map<String, PersistentQueriesInSharedRuntimesImpl> getMetadata() {
    return ImmutableMap.copyOf(metadata);
  }

  @Override
  public Map<QueryId, Set<SourceName>> getSourcesMap() {
    return ImmutableMap.copyOf(sources);
  }

  public void addQueryError(final QueryError e) {
  }
}
