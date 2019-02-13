/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.serde.DataSource;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(QueryMetadata.class);

  private final String statementString;
  private final KafkaStreams kafkaStreams;
  private final OutputNode outputNode;
  private final String executionPlan;
  private final DataSource.DataSourceType dataSourceType;
  private final String queryApplicationId;
  private final Topology topology;
  private final Map<String, Object> streamsProperties;
  private final Map<String, Object> overriddenProperties;
  private final Consumer<QueryMetadata> closeCallback;
  private final Set<String> sourceNames;

  private Optional<QueryStateListener> queryStateListener = Optional.empty();
  private boolean everStarted = false;

  protected QueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final OutputNode outputNode,
      final String executionPlan,
      final DataSource.DataSourceType dataSourceType,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback
  ) {
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.kafkaStreams = Objects.requireNonNull(kafkaStreams, "kafkaStreams");
    this.outputNode = Objects.requireNonNull(outputNode, "outputNode");
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

    final PlanSourceExtractorVisitor<?, ?> visitor = new PlanSourceExtractorVisitor<>();
    visitor.process(outputNode, null);
    this.sourceNames = visitor.getSourceNames();
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

  public OutputNode getOutputNode() {
    return outputNode;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  public DataSource.DataSourceType getDataSourceType() {
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

  public Schema getResultSchema() {
    return outputNode.getSchema();
  }

  public Set<String> getSourceNames() {
    return sourceNames;
  }

  public boolean hasEverBeenStarted() {
    return everStarted;
  }

  public void close() {
    kafkaStreams.close();

    kafkaStreams.cleanUp();

    queryStateListener.ifPresent(QueryStateListener::close);

    closeCallback.accept(this);
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
