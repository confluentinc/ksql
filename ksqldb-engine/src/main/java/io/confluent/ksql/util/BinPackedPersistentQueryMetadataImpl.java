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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.QueryMetadataImpl.TimeBoundedQueue;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinPackedPersistentQueryMetadataImpl implements PersistentQueryMetadata {

  private static final Logger LOG = LoggerFactory
      .getLogger(BinPackedPersistentQueryMetadataImpl.class);

  private final KsqlConstants.PersistentQueryType persistentQueryType;
  private final String statementString;
  private final String executionPlan;
  private final String applicationId;
  private final Optional<MaterializationInfo> materializationInfo;
  private final KeyFormat keyFormat;
  private NamedTopology topology;
  private final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime;
  private final QuerySchemas schemas;
  private final ImmutableMap<String, Object> overriddenProperties;
  private final Set<DataSource> sources;
  private final QueryId queryId;
  private final Optional<DataSource> sinkDataSource;
  private final ProcessingLogger processingLogger;
  private final ExecutionStep<?> physicalPlan;
  private final PhysicalSchema resultSchema;
  private final Listener listener;
  private final Function<SharedKafkaStreamsRuntime, NamedTopology> namedTopologyBuilder;
  private final TimeBoundedQueue queryErrors;
  private final MaterializationProviderBuilderFactory
      materializationProviderBuilderFactory;
  private final Optional<ScalablePushRegistry> scalablePushRegistry;
  private final ProcessingLoggerFactory loggerFactory;
  public boolean everStarted = false;
  private boolean isPaused = false;
  private boolean corruptionCommandTopic = false;


  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  public BinPackedPersistentQueryMetadataImpl(
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementString,
      final PhysicalSchema schema,
      final Set<DataSource> sources,
      final String executionPlan,
      final String applicationId,
      final NamedTopology topology,
      final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime,
      final QuerySchemas schemas,
      final Map<String, Object> overriddenProperties,
      final QueryId queryId,
      final Optional<MaterializationInfo> materializationInfo,
      final MaterializationProviderBuilderFactory materializationProviderBuilderFactory,
      final ExecutionStep<?> physicalPlan,
      final ProcessingLogger processingLogger,
      final Optional<DataSource> sinkDataSource,
      final Listener listener,
      final Optional<ScalablePushRegistry> scalablePushRegistry,
      final Function<SharedKafkaStreamsRuntime, NamedTopology> namedTopologyBuilder,
      final KeyFormat keyFormat,
      final ProcessingLoggerFactory loggerFactory) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.persistentQueryType = Objects.requireNonNull(persistentQueryType, "persistentQueryType");
    this.statementString = Objects.requireNonNull(statementString, "statementString");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
    this.topology = Objects.requireNonNull(topology, "namedTopology");
    this.sharedKafkaStreamsRuntime =
        Objects.requireNonNull(sharedKafkaStreamsRuntime, "sharedKafkaStreamsRuntime");
    this.sinkDataSource = requireNonNull(sinkDataSource, "sinkDataSource");
    this.schemas = requireNonNull(schemas, "schemas");
    this.overriddenProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(overriddenProperties, "overriddenProperties"));
    this.sources = Objects.requireNonNull(sources, "sourceNames");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    this.physicalPlan = requireNonNull(physicalPlan, "physicalPlan");
    this.resultSchema = requireNonNull(schema, "schema");
    this.materializationProviderBuilderFactory = requireNonNull(
        materializationProviderBuilderFactory, "materializationProviderBuilderFactory");
    this.materializationInfo = requireNonNull(materializationInfo, "materializationInfo");
    this.listener = new QueryListenerWrapper(listener, scalablePushRegistry);
    this.namedTopologyBuilder = requireNonNull(namedTopologyBuilder, "namedTopologyBuilder");
    this.queryErrors = sharedKafkaStreamsRuntime.getNewQueryErrorQueue();
    this.scalablePushRegistry = requireNonNull(scalablePushRegistry, "scalablePushRegistry");
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.loggerFactory = requireNonNull(loggerFactory, "loggerFactory");
  }

  // for creating sandbox instances
  public BinPackedPersistentQueryMetadataImpl(
          final BinPackedPersistentQueryMetadataImpl original,
          final QueryMetadata.Listener listener
  ) {
    this.persistentQueryType = original.getPersistentQueryType();
    this.statementString = original.statementString;
    this.executionPlan = original.executionPlan;
    this.applicationId = original.applicationId;
    this.topology = original.topology;
    this.sharedKafkaStreamsRuntime = original.sharedKafkaStreamsRuntime;
    this.sinkDataSource = original.getSink();
    this.schemas = original.schemas;
    this.overriddenProperties =
            ImmutableMap.copyOf(original.getOverriddenProperties());
    this.sources = original.getSources();
    this.queryId = original.getQueryId();
    this.processingLogger = original.processingLogger;
    this.physicalPlan = original.getPhysicalPlan();
    this.resultSchema = original.resultSchema;
    this.materializationProviderBuilderFactory = original.materializationProviderBuilderFactory;
    this.materializationInfo = original.materializationInfo;
    this.listener = requireNonNull(listener, "listener");
    this.queryErrors = sharedKafkaStreamsRuntime.getNewQueryErrorQueue();
    this.scalablePushRegistry = original.scalablePushRegistry;
    this.namedTopologyBuilder = original.namedTopologyBuilder;
    this.keyFormat = original.keyFormat;
    this.loggerFactory = original.loggerFactory;
  }

  @Override
  public Optional<DataSource.DataSourceType> getDataSourceType() {
    return sinkDataSource.map(DataSource::getDataSourceType);
  }

  @Override
  public Optional<KsqlTopic> getResultTopic() {
    return sinkDataSource.map(DataSource::getKsqlTopic);
  }

  @Override
  public Optional<SourceName> getSinkName() {
    return sinkDataSource.map(DataSource::getName);
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
  public Optional<DataSource> getSink() {
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
    return this.materializationInfo.map(info ->
        materializationProviderBuilderFactory.materializationProviderBuilder(
            info,
            resultSchema,
            keyFormat,
            getStreamsProperties(),
            applicationId,
            this.queryId.toString()
        )
    ).flatMap(builder -> builder.apply(
            sharedKafkaStreamsRuntime.getKafkaStreams(),
            topology)
    ).map(builder -> builder.build(queryId, contextStacker));
  }

  @Override
  public void stop() {
    stop(true);
  }

  public void stop(final boolean resetOffsets) {
    sharedKafkaStreamsRuntime.stop(queryId, resetOffsets);
    scalablePushRegistry.ifPresent(ScalablePushRegistry::close);
  }

  @Override
  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable error) {
    // handler is defined in the SharedKafkaStreamsRuntime
    throw new UnsupportedOperationException("Should not get uncaught exception handler for"
                                                + " individual queries in shared runtime");
  }

  @Override
  public Optional<ScalablePushRegistry> getScalablePushRegistry() {
    return scalablePushRegistry;
  }

  @Override
  public void initialize() {

  }

  @Override
  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return sharedKafkaStreamsRuntime.getAllTaskMetadataForQuery(queryId);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "overriddenProperties is immutable")
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
    // handler has already been set on the shared runtime of bin-packed queries
    throw new UnsupportedOperationException("Should not set uncaught exception handler for"
                                                + " individual queries in shared runtime");
  }

  @Override
  public KafkaStreams.State getState() {
    if (corruptionCommandTopic) {
      return KafkaStreams.State.ERROR;
    }
    return sharedKafkaStreamsRuntime.state();
  }

  @Override
  public String getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public String getQueryApplicationId() {
    return applicationId;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "topology is for reference")
  @Override
  public NamedTopology getTopology() {
    return topology;
  }

  public NamedTopology getTopologyCopy(final SharedKafkaStreamsRuntime builder) {
    return namedTopologyBuilder.apply(builder);
  }

  public void updateTopology(final NamedTopology topology) {
    this.topology = topology;
  }

  @Override
  public Map<String, Map<Integer, LagInfo>> getAllLocalStorePartitionLags() {
    return sharedKafkaStreamsRuntime.getAllLocalStorePartitionLagsForQuery(queryId);
  }

  @Override
  public Collection<StreamsMetadata> getAllStreamsHostMetadata() {
    try {
      return ImmutableList.copyOf(
          sharedKafkaStreamsRuntime.getAllStreamsClientsMetadataForQuery(queryId));
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
    return ImmutableSet.copyOf(sources.stream()
        .map(DataSource::getName)
        .collect(Collectors.toSet()));
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
    return queryErrors.toImmutableList();
  }

  @Override
  public void setCorruptionQueryError() {
    final QueryError corruptionQueryError = new QueryError(
        System.currentTimeMillis(),
        "Query not started due to corruption in the command topic.",
        QueryError.Type.USER
    );
    setQueryError(corruptionQueryError);
    corruptionCommandTopic = true;
  }

  public void setQueryError(final QueryError error) {
    listener.onError(this, error);
    queryErrors.add(error);
  }

  @Override
  public KafkaStreams getKafkaStreams() {
    return sharedKafkaStreamsRuntime.getKafkaStreams();
  }

  public void onStateChange(final State newState, final State oldState) {
    listener.onStateChange(this, newState, oldState);
  }

  @Override
  public void close() {
    loggerFactory.getLoggersWithPrefix(queryId.toString()).forEach(ProcessingLogger::close);
    sharedKafkaStreamsRuntime.stop(queryId, true);
    scalablePushRegistry.ifPresent(ScalablePushRegistry::close);
    listener.onClose(this);
  }

  @Override
  public void start() {
    if (!everStarted) {
      sharedKafkaStreamsRuntime.start(queryId);
    }
    everStarted = true;
  }

  @Override
  public KsqlQueryStatus getQueryStatus() {
    if (isPaused) {
      return KsqlQueryStatus.PAUSED;
    } else {
      return KsqlConstants.fromStreamsState(getState());
    }
  }

  @Override
  public void pause() {
    sharedKafkaStreamsRuntime.getKafkaStreams().pauseNamedTopology(topology.name());
    isPaused = true;
    listener.onPause(this);
  }

  @Override
  public void resume() {
    sharedKafkaStreamsRuntime.getKafkaStreams().resumeNamedTopology(topology.name());
    isPaused = false;
    listener.onPause(this);
  }

  @Override
  public void register() {
    sharedKafkaStreamsRuntime.register(
        this
    );
  }

  Listener getListener() {
    return listener;
  }

  @Override
  public Collection<String> getSourceTopicNames() {
    return sources.stream()
        .map(s -> s.getKsqlTopic()
            .getKafkaTopicName())
        .collect(Collectors.toSet());
  }

  private Set<DataSource> getSources() {
    return sources;
  }

}