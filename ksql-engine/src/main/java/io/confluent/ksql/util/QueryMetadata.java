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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(QueryMetadata.class);

  private final String statementString;
  private final KafkaStreams kafkaStreams;
  private final String executionPlan;
  private final DataSourceType dataSourceType;
  private final String queryApplicationId;
  private final Topology topology;
  private final Map<String, Object> streamsProperties;
  private final Map<String, Object> overriddenProperties;
  private final Consumer<QueryMetadata> closeCallback;
  private final Set<String> sourceNames;
  private final KsqlSchema schema;

  private Optional<QueryStateListener> queryStateListener = Optional.empty();
  private boolean everStarted = false;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  protected QueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final KsqlSchema schema,
      final Set<String> sourceNames,
      final String executionPlan,
      final DataSourceType dataSourceType,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.kafkaStreams = Objects.requireNonNull(kafkaStreams, "kafkaStreams");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.dataSourceType = Objects.requireNonNull(dataSourceType, "dataSourceType");
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
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  protected QueryMetadata(final QueryMetadata other, final Consumer<QueryMetadata> closeCallback) {
    this.statementString = other.statementString;
    this.kafkaStreams = other.kafkaStreams;
    this.executionPlan = other.executionPlan;
    this.dataSourceType = other.dataSourceType;
    this.queryApplicationId = other.queryApplicationId;
    this.topology = other.topology;
    this.streamsProperties = other.streamsProperties;
    this.overriddenProperties = other.overriddenProperties;
    this.sourceNames = other.sourceNames;
    this.schema = other.schema;
    this.closeCallback = Objects.requireNonNull(closeCallback, "closeCallback");
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

  public DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  public Topology getTopology() {
    return topology;
  }

  public Map<String, Object> getStreamsProperties() {
    return streamsProperties;
  }

  public KsqlSchema getResultSchema() {
    return schema;
  }

  public Set<String> getSourceNames() {
    return sourceNames;
  }

  public boolean hasEverBeenStarted() {
    return everStarted;
  }


  /**
   * Stops the query without cleaning up the external resources
   * so that it can be resumed when we call {@link #start()}.
   *
   * <p>NOTE: {@link QueuedQueryMetadata} overrides this method
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
    kafkaStreams.close();

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
}
