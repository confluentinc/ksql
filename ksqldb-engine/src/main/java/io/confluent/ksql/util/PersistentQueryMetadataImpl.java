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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Metadata of a persistent query, e.g. {@code CREATE STREAM FOO AS SELECT * FROM BAR;}.
 */
public class PersistentQueryMetadataImpl
    extends QueryMetadataImpl implements PersistentQueryMetadata {
  private final KsqlConstants.PersistentQueryType persistentQueryType;
  private final Optional<DataSource> sinkDataSource;
  private final QuerySchemas schemas;
  private final PhysicalSchema resultSchema;
  private final ExecutionStep<?> physicalPlan;
  private final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
      materializationProviderBuilder;
  private final Optional<ScalablePushRegistry> scalablePushRegistry;
  private final ProcessingLogger processingLogger;

  private Optional<MaterializationProvider> materializationProvider;
  private final ScheduledExecutorService executorService;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public PersistentQueryMetadataImpl(
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementString,
      final PhysicalSchema schema,
      final Set<SourceName> sourceNames,
      final Optional<DataSource> sinkDataSource,
      final String executionPlan,
      final QueryId id,
      final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
          materializationProviderBuilder,
      final String queryApplicationId,
      final Topology topology,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final QuerySchemas schemas,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final long closeTimeout,
      final QueryErrorClassifier errorClassifier,
      final ExecutionStep<?> physicalPlan,
      final int maxQueryErrorsQueueSize,
      final ProcessingLogger processingLogger,
      final long retryBackoffInitialMs,
      final long retryBackoffMaxMs,
      final QueryMetadata.Listener listener,
      final Optional<ScalablePushRegistry> scalablePushRegistry,
      final ProcessingLoggerFactory loggerFactory
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        schema.logicalSchema(),
        sourceNames,
        executionPlan,
        queryApplicationId,
        topology,
        kafkaStreamsBuilder,
        streamsProperties,
        overriddenProperties,
        closeTimeout,
        id,
        errorClassifier,
        maxQueryErrorsQueueSize,
        retryBackoffInitialMs,
        retryBackoffMaxMs,
        new QueryListenerWrapper(listener, scalablePushRegistry),
        loggerFactory
    );
    this.sinkDataSource = requireNonNull(sinkDataSource, "sinkDataSource");
    this.schemas = requireNonNull(schemas, "schemas");
    this.resultSchema = requireNonNull(schema, "schema");
    this.physicalPlan = requireNonNull(physicalPlan, "physicalPlan");
    this.materializationProviderBuilder =
        requireNonNull(materializationProviderBuilder, "materializationProviderBuilder");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    this.scalablePushRegistry = requireNonNull(scalablePushRegistry, "scalablePushRegistry");
    this.persistentQueryType = requireNonNull(persistentQueryType, "persistentQueryType");
    this.executorService =
      Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat("ksql-csu-metrics-reporter-%d").build()
      );
  }

  // for creating sandbox instances
  protected PersistentQueryMetadataImpl(
      final PersistentQueryMetadataImpl original,
      final QueryMetadata.Listener listener
  ) {
    super(original, listener);
    this.sinkDataSource = original.getSink();
    this.schemas = original.schemas;
    this.resultSchema = original.resultSchema;
    this.materializationProvider = original.materializationProvider;
    this.physicalPlan = original.getPhysicalPlan();
    this.materializationProviderBuilder = original.materializationProviderBuilder;
    this.processingLogger = original.processingLogger;
    this.scalablePushRegistry = original.scalablePushRegistry;
    this.persistentQueryType = original.getPersistentQueryType();
    this.executorService = original.executorService;
  }

  @Override
  public void initialize() {
    // initialize the first KafkaStreams
    super.initialize();
    setUncaughtExceptionHandler(this::uncaughtHandler);

    this.materializationProvider = materializationProviderBuilder
        .flatMap(builder -> builder.apply(getKafkaStreams(), getTopology()));
  }

  @Override
  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable error
  ) {
    final StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse response =
            super.uncaughtHandler(error);

    processingLogger.error(KafkaStreamsThreadError.of(
        "Unhandled exception caught in streams thread", Thread.currentThread(), error));
    return response;
  }

  @Override
  public Optional<DataSourceType> getDataSourceType() {
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

  public QuerySchemas getQuerySchemas() {
    return schemas;
  }

  public PhysicalSchema getPhysicalSchema() {
    return resultSchema;
  }

  public ExecutionStep<?> getPhysicalPlan() {
    return physicalPlan;
  }

  @Override
  public Optional<DataSource> getSink() {
    return sinkDataSource;
  }

  public KsqlConstants.PersistentQueryType getPersistentQueryType() {
    return persistentQueryType;
  }

  @VisibleForTesting
  public ProcessingLogger getProcessingLogger() {
    return processingLogger;
  }

  public Optional<Materialization> getMaterialization(
      final QueryId queryId,
      final QueryContext.Stacker contextStacker
  ) {
    return materializationProvider.map(builder -> builder.build(queryId, contextStacker));
  }

  /**
   * Stops the query without cleaning up the external resources
   * so that it can be resumed when we call {@link #start()}.
   *
   * @see #close()
   */
  public synchronized void stop() {
    doClose(false);
    scalablePushRegistry.ifPresent(ScalablePushRegistry::close);
  }

  public void stop(final boolean resetOffsets) {
    stop();
  }

  @Override
  public void register() {

  }

  public Optional<ScalablePushRegistry> getScalablePushRegistry() {
    return scalablePushRegistry;
  }

  public Collection<String> getSourceTopicNames() {
    return Collections.emptySet();
  }

}
