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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public abstract class SharedKafkaStreamsRuntime {
  private final Logger log = LoggerFactory.getLogger(SharedKafkaStreamsRuntime.class);

  protected final QueryErrorClassifier  errorClassifier;
  protected final KafkaStreamsNamedTopologyWrapper kafkaStreams;
  protected final Map<String, Object> streamsProperties;
  protected final QueryMetadataImpl.TimeBoundedQueue runtimeErrors;
  protected final Map<QueryId, Set<SourceName>> sources;
  protected final Map<QueryId, SharedRuntimePersistentQueryMetadata> queriesInSharedRuntime;

  public SharedKafkaStreamsRuntime(final KafkaStreamsBuilder kafkaStreamsBuilder,
                                   final int maxQueryErrorsQueueSize,
                                   final QueryErrorClassifier errorClassifier,
                                   final Map<String, Object> streamsProperties) {
    this(kafkaStreamsBuilder,
         errorClassifier,
         streamsProperties,
         new QueryMetadataImpl.TimeBoundedQueue(Duration.ofHours(1), maxQueryErrorsQueueSize),
         new ConcurrentHashMap<>(),
         new ConcurrentHashMap<>()
    );
  }

  protected SharedKafkaStreamsRuntime(
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final QueryErrorClassifier errorClassifier,
      final Map<String, Object> streamsProperties,
      final QueryMetadataImpl.TimeBoundedQueue runtimeErrors,
      final Map<QueryId, Set<SourceName>> sources,
      final Map<QueryId, SharedRuntimePersistentQueryMetadata> queriesInSharedRuntime) {
    this.kafkaStreams = kafkaStreamsBuilder.buildNamedTopologyWrapper(streamsProperties);
    this.runtimeErrors = runtimeErrors;
    this.errorClassifier = errorClassifier;
    this.streamsProperties = ImmutableMap.copyOf(streamsProperties);
    this.queriesInSharedRuntime = queriesInSharedRuntime;
    this.sources = sources;
    kafkaStreams.setUncaughtExceptionHandler(this::uncaughtHandler);
  }

  public void reserveRuntime(final QueryId queryId, final Set<SourceName> sourceNames) {
    sources.put(queryId, sourceNames);
    log.info("Registering sources {} for query {} with current sources by query: {}",
        sourceNames, queryId.toString(), sources
    );
  }

  public void register(final SharedRuntimePersistentQueryMetadata persistentQueryMetadata) {
    final QueryId queryId = persistentQueryMetadata.getQueryId();
    final Set<SourceName> newQuerySourceTopics = persistentQueryMetadata.getSourceNames();
    final Set<SourceName> oldQuerySourceTopics = sources.get(queryId);

    if (oldQuerySourceTopics == null || !oldQuerySourceTopics.equals(newQuerySourceTopics)) {
      final List<Map.Entry<QueryId, SourceName>> otherQueriesSourceTopics = sources.entrySet()
          .stream()
          .map(q -> q
              .getValue().stream()
              .map(s -> new SimpleEntry<>(q.getKey(), s))
              .collect(Collectors.toSet()))
          .flatMap(Collection::stream)
          .collect(Collectors.toList());

      for (final Map.Entry<QueryId, SourceName> query : otherQueriesSourceTopics) {
        final SourceName source = query.getValue();
        if (newQuerySourceTopics.contains(source)) {
          log.error("Failed to register query {} because query {} is already registered"
                        + "with source topic {}", queryId, query.getKey(), source);
          throw new IllegalStateException(queryId + " cannot be registered with this runtime");
        } else {
          sources.put(queryId, persistentQueryMetadata.getSourceNames());
        }
      }
    }

    queriesInSharedRuntime.put(queryId, persistentQueryMetadata);
  }

  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable e
  ) {
    QueryError.Type errorType = QueryError.Type.UNKNOWN;
    try {
      errorType = errorClassifier.classify(e);
    } catch (final Exception classificationException) {
      log.error("Error classifying unhandled exception", classificationException);
    } finally {
      // If error classification throws then we consider the error to be an UNKNOWN error.
      // We notify listeners and add the error to the errors queue in the finally block to ensure
      // all listeners and consumers of the error queue (eg the API) can see the error. Similarly,
      // log in finally block to make sure that if there's ever an error in the classification
      // we still get this in our logs.
      final QueryError queryError =
          new QueryError(
              System.currentTimeMillis(),
              Throwables.getStackTraceAsString(e),
              errorType
          );

      final String queryId;
      if (e instanceof StreamsException && ((StreamsException) e).taskId().isPresent()) {
        queryId = ((StreamsException) e).taskId().get().topologyName();
      } else {
        queryId = null;
      }

      if (queryId == null) {
        for (SharedRuntimePersistentQueryMetadata query : queriesInSharedRuntime.values()) {
          query.getListener().onError(queriesInSharedRuntime.get(query.getQueryId()), queryError);
        }
        addRuntimeError(queryError);
        log.error(String.format(
            "Unhandled exception caught in streams thread %s. (%s)",
            Thread.currentThread().getName(),
            errorType),
                  e
        );
      } else {
        queriesInSharedRuntime.get(new QueryId(queryId)).addQueryError(queryError);
        log.error(String.format(
            "Unhandled exception caught in streams thread %s for query %s. (%s)",
            Thread.currentThread().getName(),
            errorType,
            queryId),
                  e
        );
      }
    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streams must be exposed")
  public KafkaStreamsNamedTopologyWrapper getKafkaStreams() {
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
    return !(runtimeErrors.toImmutableList().isEmpty()
            && queriesInSharedRuntime.get(queryId).getQueryErrors().isEmpty());
  }

  public abstract void stop(QueryId queryId);

  public abstract void close(boolean cleanupResources);

  public abstract void start(QueryId queryId);

  public List<QueryError> getRuntimeErrors() {
    return runtimeErrors.toImmutableList();
  }

  public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(final QueryId queryId) {
    try {
      return kafkaStreams.allLocalStorePartitionLags();
    } catch (final IllegalStateException | StreamsException e) {
      log.error(e.getMessage());
      return ImmutableMap.of();
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streamsProperties is immutable")
  public Map<String, Object> getStreamProperties() {
    return streamsProperties;
  }

  public Set<QueryId> getQueries() {
    return ImmutableSet.copyOf(sources.keySet());
  }

  public int numberOfQueries() {
    return queriesInSharedRuntime.size();
  }

  public Map<QueryId, SharedRuntimePersistentQueryMetadata> getQueriesInSharedRuntime() {
    return ImmutableMap.copyOf(queriesInSharedRuntime);
  }

  public String getApplicationId() {
    return streamsProperties.get(StreamsConfig.APPLICATION_ID_CONFIG).toString();
  }

  public Set<SourceName> getSources() {
    return ImmutableSet.copyOf(
        sources
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toSet()));
  }

  public Map<QueryId, Set<SourceName>> getSourcesMap() {
    return ImmutableMap.copyOf(sources);
  }

  public void addRuntimeError(final QueryError e) {
    runtimeErrors.add(e);
  }
}
