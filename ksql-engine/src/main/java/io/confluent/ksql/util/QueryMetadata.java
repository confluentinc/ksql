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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(QueryMetadata.class);

  private static final Field STREAMS_INTERNAL_FIELD = getStreamsInternalField();

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
  private final StreamsMetadataState streamsMetadataState;

  private Optional<QueryStateListener> queryStateListener = Optional.empty();
  private boolean everStarted = false;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public QueryMetadata(
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
      final long closeTimeout
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this(
        statementString,
        kafkaStreams,
        getStreamsMetadataState(kafkaStreams),
        logicalSchema,
        sourceNames,
        executionPlan,
        queryApplicationId,
        topology,
        streamsProperties,
        overriddenProperties,
        closeCallback,
        closeTimeout
    );
  }

  @VisibleForTesting
    // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  QueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final StreamsMetadataState streamsMetadataState,
      final LogicalSchema logicalSchema,
      final Set<SourceName> sourceNames,
      final String executionPlan,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback,
      final long closeTimeout
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.statementString = requireNonNull(statementString, "statementString");
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.streamsMetadataState = requireNonNull(streamsMetadataState, "streamsMetadataState");
    this.executionPlan = requireNonNull(executionPlan, "executionPlan");
    this.queryApplicationId = requireNonNull(queryApplicationId, "queryApplicationId");
    this.topology = requireNonNull(topology, "kafkaTopicClient");
    this.streamsProperties =
        ImmutableMap.copyOf(
            requireNonNull(streamsProperties, "streamsPropeties"));
    this.overriddenProperties =
        ImmutableMap.copyOf(
            requireNonNull(overriddenProperties, "overriddenProperties"));
    this.closeCallback = requireNonNull(closeCallback, "closeCallback");
    this.sourceNames = requireNonNull(sourceNames, "sourceNames");
    this.logicalSchema = requireNonNull(logicalSchema, "logicalSchema");
    this.closeTimeout = closeTimeout;
  }

  protected QueryMetadata(final QueryMetadata other, final Consumer<QueryMetadata> closeCallback) {
    this.statementString = other.statementString;
    this.kafkaStreams = other.kafkaStreams;
    this.streamsMetadataState = other.streamsMetadataState;
    this.executionPlan = other.executionPlan;
    this.queryApplicationId = other.queryApplicationId;
    this.topology = other.topology;
    this.streamsProperties = other.streamsProperties;
    this.overriddenProperties = other.overriddenProperties;
    this.sourceNames = other.sourceNames;
    this.logicalSchema = other.logicalSchema;
    this.closeCallback = requireNonNull(closeCallback, "closeCallback");
    this.closeTimeout = other.closeTimeout;
  }

  public void registerQueryStateListener(final QueryStateListener queryStateListener) {
    this.queryStateListener = Optional.of(queryStateListener);
    queryStateListener.onChange(kafkaStreams.state(), kafkaStreams.state());
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

  public String getState() {
    return kafkaStreams.state().toString();
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
    Map<String, Map<Integer, LagInfo>> getLagMap = null;
    try {
      getLagMap = kafkaStreams.allLocalStorePartitionLags();
    } catch (IllegalStateException | StreamsException e) {
      LOG.error(e.getMessage());
    }
    return getLagMap;
  }

  public Collection<StreamsMetadata> getAllMetadata() {
    try {
      // Synchronized block need until https://issues.apache.org/jira/browse/KAFKA-9668 fixed.
      synchronized (streamsMetadataState) {
        return ImmutableList.copyOf(kafkaStreams.allMetadata());
      }
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

  /*
  Use reflection to get at StreamsMetadataState, which is needed to synchronize on if ksql is to
  avoid the ConcurrentMod exception caused by this bug:
  https://issues.apache.org/jira/browse/KAFKA-9668.

  Yes, this is brittle. But it can be removed once the above bug is fixed.
   */
  static StreamsMetadataState getStreamsMetadataState(final KafkaStreams kafkaStreams) {
    try {
      return (StreamsMetadataState) STREAMS_INTERNAL_FIELD.get(kafkaStreams);
    } catch (final IllegalAccessException e) {
      throw new IllegalStateException("Failed to access KafkaStreams.streamsMetadataState", e);
    }
  }

  private static Field getStreamsInternalField() {
    try {
      final Field field = KafkaStreams.class.getDeclaredField("streamsMetadataState");
      field.setAccessible(true);
      return field;
    } catch (final NoSuchFieldException e) {
      throw new IllegalStateException("Failed to get KafkaStreams.streamsMetadataState", e);
    }
  }
}
