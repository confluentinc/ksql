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
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.QueryMetadataImpl.TimeBoundedQueue;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedKafkaStreamsRuntimeImpl extends SharedKafkaStreamsRuntime {

  private final Logger log = LoggerFactory.getLogger(SharedKafkaStreamsRuntimeImpl.class);

  private final long shutdownTimeout;
  private final QueryErrorClassifier errorClassifier;
  private final int maxQueryErrorsQueueSize;

  public SharedKafkaStreamsRuntimeImpl(final KafkaStreamsBuilder kafkaStreamsBuilder,
                                       final QueryErrorClassifier errorClassifier,
                                       final int maxQueryErrorsQueueSize,
                                       final long shutdownTimeoutConfig,
                                       final Map<String, Object> streamsProperties) {
    super(
        kafkaStreamsBuilder,
        streamsProperties
    );
    this.errorClassifier = errorClassifier;
    this.maxQueryErrorsQueueSize = maxQueryErrorsQueueSize;
    kafkaStreams.start();
    shutdownTimeout = shutdownTimeoutConfig;
  }

  @Override
  public void register(
      final BinPackedPersistentQueryMetadataImpl binpackedPersistentQueryMetadata,
      final QueryId queryId
  ) {
    if (!sources.containsKey(queryId)) {
      if (sources
          .values()
          .stream()
          .flatMap(Collection::stream)
          .anyMatch(t -> binpackedPersistentQueryMetadata.getSourceNames().contains(t))) {
        throw new IllegalArgumentException(
            queryId.toString() + ": was not reserved on this runtime");
      } else {
        sources.put(queryId, binpackedPersistentQueryMetadata.getSourceNames());
      }
    }
    collocatedQueries.put(queryId, binpackedPersistentQueryMetadata);
    log.info("mapping {}", collocatedQueries);
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

      final BinPackedPersistentQueryMetadataImpl queryInError = parseException(e);

      if (queryInError != null) {
        queryInError.setQueryError(queryError);
        log.error(String.format(
            "Unhandled query exception caught in streams thread %s for query %s. (%s)",
            Thread.currentThread().getName(),
            queryInError.getQueryId(),
            errorType),
                  e
        );
      } else {
        for (BinPackedPersistentQueryMetadataImpl query : collocatedQueries.values()) {
          query.setQueryError(queryError);
        }
        log.error(String.format(
            "Unhandled runtime exception caught in streams thread %s. (%s)",
            Thread.currentThread().getName(),
            errorType),
                  e
        );
      }
    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private BinPackedPersistentQueryMetadataImpl parseException(final Throwable e) {
    final TaskId task =
        e instanceof StreamsException && ((StreamsException) e).taskId().isPresent()
            ? ((StreamsException) e).taskId().get()
            : null;

    final QueryId queryId
        = task != null && task.topologyName() != null
        ? new QueryId(task.topologyName())
        : null;

    if (task != null && task.topologyName() == null) {
      log.error("Unhandled exception originated from a task {}"
                    + " without an associated topology name (queryId).", task);
    } else if (queryId != null && !collocatedQueries.containsKey(queryId)) {
      log.error("Unhandled exception originated from a task {}"
                    + " with an unrecognized topology name (queryId) {}.", task, queryId);
    }

    if (queryId != null && collocatedQueries.containsKey(queryId)) {
      return collocatedQueries.get(queryId);
    } else {
      return null;
    }
  }
  // CHECKSTYLE_RULES.ON: CyclomaticComplexity

  @Override
  public TimeBoundedQueue getNewQueryErrorQueue() {
    return new QueryMetadataImpl.TimeBoundedQueue(Duration.ofHours(1), maxQueryErrorsQueueSize);
  }

  @Override
  public void stop(final QueryId queryId, boolean resetOffsets) {
    log.info("Attempting to stop Query: " + queryId.toString());
    if (collocatedQueries.containsKey(queryId) && sources.containsKey(queryId)) {
      if (kafkaStreams.state().isRunningOrRebalancing()) {
        try {
          kafkaStreams.removeNamedTopology(queryId.toString(), resetOffsets)
              .all()
              .get(shutdownTimeout, TimeUnit.SECONDS);
          if (resetOffsets) {
            kafkaStreams.cleanUpNamedTopology(queryId.toString());
          }
        } catch (final TimeoutException | ExecutionException | InterruptedException e) {
          log.error("Failed to close query {} within the allotted timeout {} due to",
              queryId,
              shutdownTimeout,
              e);
          if (e instanceof TimeoutException) {
            log.warn(
                "query has not terminated even after trying to remove the topology. "
                    + "This may happen when streams threads are hung. State: "
                    + kafkaStreams.state());
          }
        }
      } else {
        throw new IllegalStateException("Streams in not running but is in state "
            + kafkaStreams.state());
      }
    }
  }

  @Override
  public synchronized void close() {
    kafkaStreams.close();
    kafkaStreams.cleanUp();
  }

  @Override
  public void start(final QueryId queryId) {
    if (collocatedQueries.containsKey(queryId) && !collocatedQueries.get(queryId).everStarted) {
      if (!kafkaStreams.getTopologyByName(queryId.toString()).isPresent()) {
        try {
          kafkaStreams.addNamedTopology(collocatedQueries.get(queryId).getTopology())
              .all()
              .get(shutdownTimeout, TimeUnit.SECONDS);
        } catch (final TimeoutException | ExecutionException | InterruptedException e) {
          log.error("Failed to start query {} within the allotted timeout {} due to",
              queryId,
              shutdownTimeout,
              e);
          throw new IllegalStateException(
              "Encountered an error when trying to add query " + queryId + " to runtime: ",
              e);
        }
      } else {
        throw new IllegalArgumentException("not done removing query: " + queryId);
      }
    } else {
      throw new IllegalArgumentException("query: " + queryId + " not added to runtime");
    }
  }

  @Override
  public void overrideStreamsProperties(final Map<String, Object> newProperties) {
    // cannot override logger
    newProperties.put(
        ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER,
        streamsProperties.get(ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER)
    );

    streamsProperties = ImmutableMap.copyOf(newProperties);
  }

  @Override
  public void restartStreamsRuntime() {
    final KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper = kafkaStreamsBuilder
        .buildNamedTopologyWrapper(streamsProperties);
    kafkaStreams.close();
    kafkaStreams = kafkaStreamsNamedTopologyWrapper;
    for (final BinPackedPersistentQueryMetadataImpl query : collocatedQueries.values()) {
      kafkaStreamsNamedTopologyWrapper.addNamedTopology(query.getTopologyCopy(this));
    }
    kafkaStreamsNamedTopologyWrapper.setUncaughtExceptionHandler(this::uncaughtHandler);
    kafkaStreamsNamedTopologyWrapper.start();
  }
}