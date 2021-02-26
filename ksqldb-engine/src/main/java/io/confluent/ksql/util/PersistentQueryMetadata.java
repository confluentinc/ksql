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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Metadata of a persistent query, e.g. {@code CREATE STREAM FOO AS SELECT * FROM BAR;}.
 */
public class PersistentQueryMetadata extends QueryMetadata {

  private final DataSource sinkDataSource;
  private final QuerySchemas schemas;
  private final PhysicalSchema resultSchema;
  private final ExecutionStep<?> physicalPlan;
  private final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
      materializationProviderBuilder;

  private Optional<MaterializationProvider> materializationProvider;
  private ProcessingLogger processingLogger;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public PersistentQueryMetadata(
      final String statementString,
      final PhysicalSchema schema,
      final Set<SourceName> sourceNames,
      final DataSource sinkDataSource,
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
      final Consumer<QueryMetadata> closeCallback,
      final long closeTimeout,
      final QueryErrorClassifier errorClassifier,
      final ExecutionStep<?> physicalPlan,
      final int maxQueryErrorsQueueSize,
      final ProcessingLogger processingLogger,
      final long retryBackoffInitialMs,
      final long retryBackoffMaxMs
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
        closeCallback,
        closeTimeout,
        id,
        errorClassifier,
        maxQueryErrorsQueueSize,
        retryBackoffInitialMs,
        retryBackoffMaxMs
    );

    this.sinkDataSource = requireNonNull(sinkDataSource, "sinkDataSource");
    this.schemas = requireNonNull(schemas, "schemas");
    this.resultSchema = requireNonNull(schema, "schema");
    this.physicalPlan = requireNonNull(physicalPlan, "physicalPlan");
    this.materializationProviderBuilder =
        requireNonNull(materializationProviderBuilder, "materializationProviderBuilder");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
  }

  protected PersistentQueryMetadata(
      final PersistentQueryMetadata other,
      final Consumer<QueryMetadata> closeCallback
  ) {
    super(other, closeCallback);
    this.sinkDataSource = other.sinkDataSource;
    this.schemas = other.schemas;
    this.resultSchema = other.resultSchema;
    this.materializationProvider = other.materializationProvider;
    this.physicalPlan = other.physicalPlan;
    this.materializationProviderBuilder = other.materializationProviderBuilder;
    this.processingLogger = other.processingLogger;
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
  protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
          final Throwable error
  ) {
    final StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse response =
            super.uncaughtHandler(error);

    processingLogger.error(KafkaStreamsThreadError.of(
        "Unhandled exception caught in streams thread", Thread.currentThread(), error));
    return response;
  }

  public DataSourceType getDataSourceType() {
    return sinkDataSource.getDataSourceType();
  }

  public KsqlTopic getResultTopic() {
    return sinkDataSource.getKsqlTopic();
  }

  public SourceName getSinkName() {
    return sinkDataSource.getName();
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

  public DataSource getSink() {
    return sinkDataSource;
  }

  @VisibleForTesting
  Optional<MaterializationProvider> getMaterializationProvider() {
    return materializationProvider;
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

  public synchronized void restart() {
    if (isClosed()) {
      throw new IllegalStateException(String.format(
          "Query with application id %s is already closed, cannot restart.",
          getQueryApplicationId()));
    }

    closeKafkaStreams();

    final KafkaStreams newKafkaStreams = buildKafkaStreams();
    materializationProvider = materializationProviderBuilder.flatMap(
        builder -> builder.apply(newKafkaStreams, getTopology()));

    resetKafkaStreams(newKafkaStreams);
    start();
  }
}
