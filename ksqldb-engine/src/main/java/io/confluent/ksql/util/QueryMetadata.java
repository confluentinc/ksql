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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(QueryMetadata.class);

  private final String statementString;
  private final KafkaStreams kafkaStreams;
  private final String executionPlan;
  private final String queryApplicationId;
  private final Topology topology;
  private final Map<String, Object> streamsProperties;
  private final Map<String, Object> overriddenProperties;
  private final Consumer<QueryMetadata> closeCallback;
  private final Set<SourceName> sourceNames;
  private final LogicalSchema logicalSchema;
  private final Long closeTimeout;
  private final QueryId queryId;
  private final QueryError[] queryErrorRef = new QueryError[]{null};
  private final QueryErrorClassifier errorClassifier;
  private final Set<QueryError> queryErrors = new HashSet<>();

  private Optional<QueryStateListener> queryStateListener = Optional.empty();
  private boolean everStarted = false;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  QueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final LogicalSchema logicalSchema,
      final Set<SourceName> sourceNames,
      final String executionPlan,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback,
      final long closeTimeout,
      final QueryId queryId,
      final QueryErrorClassifier errorClassifier
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.kafkaStreams = Objects.requireNonNull(kafkaStreams, "kafkaStreams");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.queryApplicationId = Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.topology = Objects.requireNonNull(topology, "kafkaTopicClient");
    this.streamsProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(streamsProperties, "streamsPropeties"));
    this.overriddenProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(overriddenProperties, "overriddenProperties"));
    this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
    this.sourceNames = Objects.requireNonNull(sourceNames, "sourceNames");
    this.logicalSchema = Objects.requireNonNull(logicalSchema, "logicalSchema");
    this.closeTimeout = closeTimeout;
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.errorClassifier = Objects.requireNonNull(errorClassifier, "errorClassifier");

    kafkaStreams.setUncaughtExceptionHandler(this::uncaughtHandler);
  }

  protected QueryMetadata(final QueryMetadata other, final Consumer<QueryMetadata> closeCallback) {
    this.statementString = other.statementString;
    this.kafkaStreams = other.kafkaStreams;
    this.executionPlan = other.executionPlan;
    this.queryApplicationId = other.queryApplicationId;
    this.topology = other.topology;
    this.streamsProperties = other.streamsProperties;
    this.overriddenProperties = other.overriddenProperties;
    this.sourceNames = other.sourceNames;
    this.logicalSchema = other.logicalSchema;
    this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
    this.closeTimeout = other.closeTimeout;
    this.queryId = other.queryId;
    this.errorClassifier = other.errorClassifier;
  }

  public void registerQueryStateListener(final QueryStateListener queryStateListener) {
    this.queryStateListener = Optional.of(queryStateListener);
    queryStateListener.onChange(kafkaStreams.state(), kafkaStreams.state());

    kafkaStreams.setUncaughtExceptionHandler(this::uncaughtHandler);
  }

  private void uncaughtHandler(final Thread t, final Throwable e) {
    LOG.error("Unhandled exception caught in streams thread {}.", t.getName(), e);
    final QueryError queryError =
        new QueryError(Throwables.getStackTraceAsString(e), errorClassifier.classify(e));
    queryStateListener.ifPresent(lis -> lis.onError(queryError));
    queryErrors.add(queryError);
  }

  public Map<String, Object> getOverriddenProperties() {
    return overriddenProperties;
  }

  public String getStatementString() {
    return statementString;
  }

  public void setUncaughtExceptionHandler(final UncaughtExceptionHandler handler) {
    kafkaStreams.setUncaughtExceptionHandler(handler);
  }

  public State getState() {
    return kafkaStreams.state();
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
  public abstract void stop();

  /**
   * Closes the {@code QueryMetadata} and cleans up any of
   * the resources associated with it (e.g. internal topics,
   * schemas, etc...).
   *
   * @see QueryMetadata#stop()
   */
  public void close() {
    doClose(true);
    closeCallback.accept(this);
  }

  protected void doClose(final boolean cleanUp) {
    kafkaStreams.close(Duration.ofMillis(closeTimeout));

    if (cleanUp) {
      kafkaStreams.cleanUp();
    }

    queryStateListener.ifPresent(QueryStateListener::close);
  }

  public void start() {
    LOG.info("Starting query with application id: {}", queryApplicationId);
    everStarted = true;
    queryStateListener.ifPresent(kafkaStreams::setStateListener);
    kafkaStreams.start();
  }

  public String getTopologyDescription() {
    return topology.describe().toString();
  }

  public List<QueryError> getQueryErrors() {
    return ImmutableList.copyOf(queryErrors);
  }

  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }
}
