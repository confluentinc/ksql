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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentQueriesInSharedRuntimesImpl implements PersistentQueryMetadata {

  private static final Logger LOG = LoggerFactory
      .getLogger(PersistentQueriesInSharedRuntimesImpl.class);

  private final KsqlConstants.PersistentQueryType persistentQueryType;
  private final String statementString;
  private final String executionPlan;
  private final String queryApplicationId;
  private final NamedTopology topology;
  private final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime;
  private final QuerySchemas schemas;
  private final Map<String, Object> overriddenProperties;
  private final Set<SourceName> sourceNames;
  private final QueryId queryId;
  private final DataSource sinkDataSource;
  private final ProcessingLogger processingLogger;
  private final ExecutionStep<?> physicalPlan;
  private final PhysicalSchema resultSchema;
  private final Listener listener;

  private static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };
  private final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
      materializationProviderBuilder;
  private final Optional<MaterializationProvider> materializationProvider;
  public boolean everStarted = false;
  private QueryErrorClassifier classifier;
  private Map<String, Object> streamsProperties;


  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  public PersistentQueriesInSharedRuntimesImpl(
        final KsqlConstants.PersistentQueryType persistentQueryType,
        final String statementString,
        final PhysicalSchema schema,
        final Set<SourceName> sourceNames,
        final String executionPlan,
        final String queryApplicationId,
        final NamedTopology topology,
        final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime,
        final QuerySchemas schemas,
        final Map<String, Object> overriddenProperties,
        final QueryId queryId,
        final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
                materializationProviderBuilder,
        final ExecutionStep<?> physicalPlan,
        final ProcessingLogger processingLogger,
        final DataSource sinkDataSource,
        final Listener listener,
        final QueryErrorClassifier classifier,
        final Map<String, Object> streamsProperties) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.persistentQueryType = Objects.requireNonNull(persistentQueryType, "persistentQueryType");
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.queryApplicationId = Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.topology = Objects.requireNonNull(topology, "kafkaTopicClient");
    this.sharedKafkaStreamsRuntime =
        Objects.requireNonNull(sharedKafkaStreamsRuntime, "sharedKafkaStreamsRuntime");
    this.sinkDataSource = requireNonNull(sinkDataSource, "sinkDataSource");
    this.schemas = requireNonNull(schemas, "schemas");
    this.overriddenProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(overriddenProperties, "overriddenProperties"));
    this.sourceNames = Objects.requireNonNull(sourceNames, "sourceNames");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    this.physicalPlan = requireNonNull(physicalPlan, "physicalPlan");
    this.resultSchema = requireNonNull(schema, "schema");
    this.materializationProviderBuilder =
        requireNonNull(materializationProviderBuilder, "materializationProviderBuilder");
    this.listener = requireNonNull(listener, "listen");
    this.materializationProvider = materializationProviderBuilder
            .flatMap(builder -> builder.apply(
                    this.sharedKafkaStreamsRuntime.getKafkaStreams(),
                    getTopology()
            ));
    this.classifier = requireNonNull(classifier, "classifier");
    this.streamsProperties = requireNonNull(streamsProperties, "streamsProperties");
  }


  // for creating sandbox instances
  protected PersistentQueriesInSharedRuntimesImpl(
          final PersistentQueriesInSharedRuntimesImpl original,
          final QueryMetadata.Listener listener
  ) {
    this.persistentQueryType = original.persistentQueryType;
    this.statementString = original.statementString;
    this.executionPlan = original.executionPlan;
    this.queryApplicationId = original.queryApplicationId;
    this.topology = original.topology;
    this.sharedKafkaStreamsRuntime = original.sharedKafkaStreamsRuntime;
    this.sinkDataSource = original.sinkDataSource;
    this.schemas = original.schemas;
    this.overriddenProperties =
            ImmutableMap.copyOf(original.overriddenProperties);
    this.sourceNames = original.sourceNames;
    this.queryId = original.queryId;
    this.processingLogger = original.processingLogger;
    this.physicalPlan = original.physicalPlan;
    this.resultSchema = original.resultSchema;
    this.materializationProviderBuilder = original.materializationProviderBuilder;
    this.listener = requireNonNull(listener, "listen");
    this.materializationProvider = original.materializationProvider;
  }

  @Override
  public DataSource.DataSourceType getDataSourceType() {
    return sinkDataSource.getDataSourceType();
  }

  @Override
  public KsqlTopic getResultTopic() {
    return sinkDataSource.getKsqlTopic();
  }

  @Override
  public SourceName getSinkName() {
    return sinkDataSource.getName();
  }

  @Override
  public QuerySchemas getQuerySchemas() {
    return schemas;
  }

  @Override
  public PhysicalSchema getPhysicalSchema() {
    return resultSchema;
  }

  @Override
  public ExecutionStep<?> getPhysicalPlan() {
    return physicalPlan;
  }

  @Override
  public DataSource getSink() {
    return sinkDataSource;
  }

  @Override
  public KsqlConstants.PersistentQueryType getPersistentQueryType() {
    return persistentQueryType;
  }

  @Override
  public ProcessingLogger getProcessingLogger() {
    return processingLogger;
  }

  @Override
  public Optional<Materialization> getMaterialization(
      final QueryId queryId,
      final QueryContext.Stacker contextStacker) {
    return materializationProvider.map(builder -> builder.build(queryId, contextStacker));
  }

  @Override
  public void stop() {
    sharedKafkaStreamsRuntime.close(queryId);
  }

  @Override
  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable error) {
    return sharedKafkaStreamsRuntime.uncaughtHandler(error);
  }

  @Override
  public Optional<MaterializationProvider> getMaterializationProvider() {
    return materializationProvider;
  }

  @Override
  public Optional<ScalablePushRegistry> getScalablePushRegistry() {
    return Optional.empty();
  }

  @Override
  public void initialize() {

  }

  @Override
  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return sharedKafkaStreamsRuntime.getTaskMetadata();
  }

  @Override
  public Map<String, Object> getOverriddenProperties() {
    return overriddenProperties;
  }

  @Override
  public String getStatementString() {
    return statementString;
  }

  @Override
  public void setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler handler) {
    //Not done but query for bin packed queries
    return;
  }

  @Override
  public KafkaStreams.State getState() {
    return sharedKafkaStreamsRuntime.state();
  }

  @Override
  public String getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  @Override
  public NamedTopology getTopology() {
    return topology;
  }

  @Override
  public Map<String, Map<Integer, LagInfo>> getAllLocalStorePartitionLags() {
    return sharedKafkaStreamsRuntime.allLocalStorePartitionLags(queryId);
  }

  @Override
  public Collection<StreamsMetadata> getAllMetadata() {
    try {
      return ImmutableList.copyOf(sharedKafkaStreamsRuntime.allMetadata());
    } catch (IllegalStateException e) {
      LOG.error(e.getMessage());
    }
    return ImmutableList.of();
  }

  @Override
  public Map<String, Object> getStreamsProperties() {
    return sharedKafkaStreamsRuntime.getStreamProperties();
  }

  @Override
  public LogicalSchema getLogicalSchema() {
    return resultSchema.logicalSchema();
  }

  @Override
  public Set<SourceName> getSourceNames() {
    return sourceNames;
  }

  @Override
  public boolean hasEverBeenStarted() {
    return everStarted;
  }

  @Override
  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public KsqlConstants.KsqlQueryType getQueryType() {
    return KsqlConstants.KsqlQueryType.PERSISTENT;
  }

  @Override
  public String getTopologyDescription() {
    return topology.describe().toString();
  }

  @Override
  public List<QueryError> getQueryErrors() {
    return sharedKafkaStreamsRuntime.getQueryErrors();
  }

  @Override
  public KafkaStreams getKafkaStreams() {
    return sharedKafkaStreamsRuntime.getKafkaStreams();
  }

  @Override
  public void close() {
    sharedKafkaStreamsRuntime.close(queryId);
    listener.onClose(this);
  }

  @Override
  public void start() {
    if (!everStarted) {
      sharedKafkaStreamsRuntime.addQuery(
          classifier,
          streamsProperties,
          this,
          queryId
      );
      sharedKafkaStreamsRuntime.start(queryId);
    }
    everStarted = true;
  }

  Listener getListener() {
    return listener;
  }
}