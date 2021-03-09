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

package io.confluent.ksql.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(QueryMetadata.class);

  private final String statementString;
  private final String executionPlan;
  private final String queryApplicationId;
  private final Topology topology;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Map<String, Object> streamsProperties;
  private final Map<String, Object> overriddenProperties;
  protected final Consumer<QueryMetadata> closeCallback;
  private final Set<SourceName> sourceNames;
  private final LogicalSchema logicalSchema;
  private final Duration closeTimeout;
  private final QueryId queryId;
  private final QueryErrorClassifier errorClassifier;
  private final TimeBoundedQueue queryErrors;
  private final RetryEvent retryEvent;

  private Optional<QueryStateListener> queryStateListener = Optional.empty();
  private boolean everStarted = false;
  protected boolean closed = false;
  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler = this::uncaughtHandler;
  private KafkaStreams kafkaStreams;
  private Consumer<Boolean> onStop = (ignored) -> { };
  private boolean initialized = false;

  private static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  QueryMetadata(
      final String statementString,
      final LogicalSchema logicalSchema,
      final Set<SourceName> sourceNames,
      final String executionPlan,
      final String queryApplicationId,
      final Topology topology,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback,
      final long closeTimeout,
      final QueryId queryId,
      final QueryErrorClassifier errorClassifier,
      final int maxQueryErrorsQueueSize,
      final long baseWaitingTimeMs,
      final long retryBackoffMaxMs
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.queryApplicationId = Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.topology = Objects.requireNonNull(topology, "kafkaTopicClient");
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder, "kafkaStreamsBuilder");
    this.streamsProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(streamsProperties, "streamsPropeties"));
    this.overriddenProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(overriddenProperties, "overriddenProperties"));
    this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
    this.sourceNames = Objects.requireNonNull(sourceNames, "sourceNames");
    this.logicalSchema = Objects.requireNonNull(logicalSchema, "logicalSchema");
    this.closeTimeout = Duration.ofMillis(closeTimeout);
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.errorClassifier = Objects.requireNonNull(errorClassifier, "errorClassifier");
    this.queryErrors = new TimeBoundedQueue(Duration.ofHours(1), maxQueryErrorsQueueSize);
    this.retryEvent = new RetryEvent(
            queryId,
            baseWaitingTimeMs,
            retryBackoffMaxMs,
            CURRENT_TIME_MILLIS_TICKER
    );
  }

  protected QueryMetadata(final QueryMetadata other, final Consumer<QueryMetadata> closeCallback) {
    this.statementString = other.statementString;
    this.kafkaStreams = other.kafkaStreams;
    this.executionPlan = other.executionPlan;
    this.queryApplicationId = other.queryApplicationId;
    this.topology = other.topology;
    this.kafkaStreamsBuilder = other.kafkaStreamsBuilder;
    this.streamsProperties = other.streamsProperties;
    this.overriddenProperties = other.overriddenProperties;
    this.sourceNames = other.sourceNames;
    this.logicalSchema = other.logicalSchema;
    this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
    this.closeTimeout = other.closeTimeout;
    this.queryId = other.queryId;
    this.errorClassifier = other.errorClassifier;
    this.uncaughtExceptionHandler = other.uncaughtExceptionHandler;
    this.queryStateListener = other.queryStateListener;
    this.everStarted = other.everStarted;
    this.queryErrors = other.queryErrors;
    this.retryEvent = other.retryEvent;
  }

  public void initialize() {
    // initialize the first KafkaStreams
    this.kafkaStreams = kafkaStreamsBuilder.build(topology, streamsProperties);
    kafkaStreams.setUncaughtExceptionHandler(this::uncaughtHandler);
    this.initialized = true;
  }

  public void setQueryStateListener(final QueryStateListener queryStateListener) {
    this.queryStateListener = Optional.of(queryStateListener);
    kafkaStreams.setStateListener(queryStateListener);
    queryStateListener.onChange(kafkaStreams.state(), kafkaStreams.state());
  }

  /**
   * Set a callback to execute when the query stops. This is run on {@link #stop()}
   * as well as {@link #close()}, unlike the {@link #closeCallback}, which is only
   * executed on {@code close}.
   *
   * <p>{@code onStop} accepts true iff the callback is being called from {@code close}.
   */
  public void onStop(final Consumer<Boolean> onStop) {
    this.onStop = onStop;
  }

  protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
          final Throwable e
  ) {
    QueryError.Type errorType = Type.UNKNOWN;
    try {
      errorType = errorClassifier.classify(e);
    } catch (final Exception classificationException) {
      LOG.error("Error classifying unhandled exception", classificationException);
    } finally {
      // If error classification throws then we consider the error to be an UNKNOWN error.
      // We notify listeners and add the error to the errors queue in the finally block to ensure
      // all listeners and consumers of the error queue (e.g. the API) can see the error. Similarly,
      // log in finally block to make sure that if there's ever an error in the classification
      // we still get this in our logs.
      final QueryError queryError =
          new QueryError(
              System.currentTimeMillis(),
              Throwables.getStackTraceAsString(e),
              errorType
          );
      queryStateListener.ifPresent(lis -> lis.onError(queryError));
      queryErrors.add(queryError);
      LOG.error(
          "Unhandled exception caught in streams thread {}. ({})",
          Thread.currentThread().getName(),
          errorType,
          e
      );
    }
    retryEvent.backOff();
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return kafkaStreams.localThreadsMetadata()
                       .stream()
                       .flatMap(t -> t.activeTasks().stream())
                       .map(StreamsTaskMetadata::fromStreamsTaskMetadata)
                       .collect(Collectors.toSet());
  }

  public Map<String, Object> getOverriddenProperties() {
    return overriddenProperties;
  }

  public String getStatementString() {
    return statementString;
  }

  public void setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler handler) {
    this.uncaughtExceptionHandler = handler;
    kafkaStreams.setUncaughtExceptionHandler(handler);
  }

  public State getState() {
    return kafkaStreams.state();
  }

  public boolean isError() {
    return getState() == State.ERROR;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  public Topology getTopology() {
    return topology;
  }

  public Map<String, Map<Integer, LagInfo>> getAllLocalStorePartitionLags() {
    try {
      return kafkaStreams.allLocalStorePartitionLags();
    } catch (IllegalStateException | StreamsException e) {
      LOG.error(e.getMessage());
      return ImmutableMap.of();
    }
  }

  public Collection<StreamsMetadata> getAllMetadata() {
    try {
      return ImmutableList.copyOf(kafkaStreams.allMetadata());
    } catch (IllegalStateException e) {
      LOG.error(e.getMessage());
    }
    return ImmutableList.of();
  }

  public Map<String, Object> getStreamsProperties() {
    return streamsProperties;
  }

  public LogicalSchema getLogicalSchema() {
    return logicalSchema;
  }

  public Set<SourceName> getSourceNames() {
    return sourceNames;
  }

  public boolean hasEverBeenStarted() {
    return everStarted;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public KsqlQueryType getQueryType() {
    return KsqlQueryType.PERSISTENT;
  }

  public String getTopologyDescription() {
    return topology.describe().toString();
  }

  public List<QueryError> getQueryErrors() {
    return queryErrors.toImmutableList();
  }

  public long uptime() {
    return queryStateListener.map(QueryStateListener::uptime).orElse(0L);
  }

  protected boolean isClosed() {
    return closed;
  }

  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }

  protected void resetKafkaStreams(final KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
    setUncaughtExceptionHandler(uncaughtExceptionHandler);
    queryStateListener.ifPresent(this::setQueryStateListener);
  }

  protected void closeKafkaStreams() {
    if (initialized) {
      kafkaStreams.close(closeTimeout);
    }
  }

  protected KafkaStreams buildKafkaStreams() {
    return kafkaStreamsBuilder.build(topology, streamsProperties);
  }

  /**
   * Stops the query without cleaning up the external resources
   * so that it can be resumed when we call {@link #start()}.
   *
   * <p>NOTE: {@link TransientQueryMetadata} overrides this method
   * since any time a transient query is stopped the external resources
   * should be cleaned up.</p>
   *
   * @see #close()
   */
  public synchronized void stop() {
    doClose(false);
  }

  /**
   * Closes the {@code QueryMetadata} and cleans up any of
   * the resources associated with it (e.g. internal topics,
   * schemas, etc...).
   *
   * @see QueryMetadata#stop()
   */
  public void close() {
    doClose(true);
  }

  private void doClose(final boolean cleanUp) {
    closed = true;
    closeKafkaStreams();

    if (cleanUp) {
      kafkaStreams.cleanUp();
    }

    queryStateListener.ifPresent(QueryStateListener::close);

    if (cleanUp) {
      closeCallback.accept(this);
    }
    onStop.accept(cleanUp);
  }

  public static class TimeBoundedQueue {
    private final Duration duration;
    private final Queue<QueryError> queue;

    TimeBoundedQueue(final Duration duration, final int capacity) {
      queue = EvictingQueue.create(capacity);
      this.duration = duration;
    }

    public void add(final QueryError e) {
      queue.add(e);
      evict();
    }

    public List<QueryError> toImmutableList() {
      evict();
      return ImmutableList.copyOf(queue);
    }
    
    private void evict() {
      while (queue.peek() != null) {
        if (queue.peek().getTimestamp() > System.currentTimeMillis() - duration.toMillis()) {
          break;
        }
        queue.poll();
      }
    }

  }

  public void start() {
    if (!initialized) {
      throw new KsqlException(
          String.format(
              "Failed to initialize query %s before starting it",
              queryApplicationId));
    }
    LOG.info("Starting query with application id: {}", queryApplicationId);
    everStarted = true;
    kafkaStreams.start();
  }

  public static class RetryEvent {
    private final Ticker ticker;
    private final QueryId queryId;

    private int numRetries = 0;
    private long waitingTimeMs;
    private long expiryTimeMs;
    private long retryBackoffMaxMs;

    RetryEvent(
            final QueryId queryId,
            final long baseWaitingTimeMs,
            final long retryBackoffMaxMs,
            final Ticker ticker
    ) {
      this.ticker = ticker;
      this.queryId = queryId;

      final long now = ticker.read();

      this.waitingTimeMs = baseWaitingTimeMs;
      this.retryBackoffMaxMs = retryBackoffMaxMs;
      this.expiryTimeMs = now + baseWaitingTimeMs;
    }

    public long nextRestartTimeMs() {
      return expiryTimeMs;
    }

    public int getNumRetries() {
      return numRetries;
    }

    public void backOff() {
      numRetries++;

      final long now = ticker.read();

      this.waitingTimeMs = getWaitingTimeMs();

      try {
        Thread.sleep(this.waitingTimeMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      LOG.info("Restarting query {} (attempt #{})", queryId, numRetries);

      // Math.max() prevents overflow if now is Long.MAX_VALUE (found just in tests)
      this.expiryTimeMs = Math.max(now, now + waitingTimeMs);
    }

    private long getWaitingTimeMs() {
      if ((waitingTimeMs * 2) < retryBackoffMaxMs) {
        return waitingTimeMs * 2;
      } else {
        return retryBackoffMaxMs;
      }
    }
  }
}
