/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.runtime;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetricsTagsUtil;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Contains all the context required to build the final runtime topology fom an execution plan.
 * Ultimately we should make this more abstract to support different runtimes, but for now the
 * assumed runtime is Kafka Streams.
 */
public final class RuntimeBuildContext {

  /**
   * System property to turn on tracking of serde operations. Must never be turned on in production
   * code as it will kill performance. Required by QueryTranslationTest.
   */
  public static final String KSQL_TEST_TRACK_SERDE_TOPICS = "ksql.test.track.serde.topics";

  private final StreamsBuilder streamsBuilder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final KeySerdeFactory keySerdeFactory;
  private final ValueSerdeFactory valueSerdeFactory;
  private final QueryId queryId;
  private final String applicationId;
  private final QuerySchemas schemas = new QuerySchemas();

  public static RuntimeBuildContext of(
      final StreamsBuilder streamsBuilder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final String applicationId,
      final QueryId queryId
  ) {
    return new RuntimeBuildContext(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        applicationId,
        queryId,
        new GenericKeySerDe(queryId.toString()),
        new GenericRowSerDe(queryId.toString())
    );
  }

  @VisibleForTesting
  RuntimeBuildContext(
      final StreamsBuilder streamsBuilder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final String applicationId,
      final QueryId queryId,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this.streamsBuilder = requireNonNull(streamsBuilder, "streamsBuilder");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.applicationId = requireNonNull(applicationId, "applicationId");
    this.queryId = requireNonNull(queryId, "queryId");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  public ProcessingLogger getProcessingLogger(final QueryContext queryContext) {
    return processingLogContext
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(queryId, queryContext),
            MetricsTagsUtil.getMetricsTagsWithQueryId(queryId.toString(), Collections.emptyMap()));
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public StreamsBuilder getStreamsBuilder() {
    return streamsBuilder;
  }

  public QuerySchemas getSchemas() {
    return schemas;
  }

  public Serde<GenericKey> buildKeySerde(
      final FormatInfo format, final PhysicalSchema schema, final QueryContext queryContext
  ) {
    final String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    schemas.trackKeySerdeCreation(
        loggerNamePrefix,
        schema.logicalSchema(),
        KeyFormat.nonWindowed(format, schema.keySchema().features())
    );

    return keySerdeFactory.create(
        format,
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext,
        getSerdeTracker(loggerNamePrefix)
    );
  }

  public Serde<Windowed<GenericKey>> buildKeySerde(
      final FormatInfo format,
      final WindowInfo window,
      final PhysicalSchema schema,
      final QueryContext queryContext
  ) {
    final String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    schemas.trackKeySerdeCreation(
        loggerNamePrefix,
        schema.logicalSchema(),
        KeyFormat.windowed(format, schema.keySchema().features(), window)
    );

    return keySerdeFactory.create(
        format,
        window,
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext,
        getSerdeTracker(loggerNamePrefix)
    );
  }

  public Serde<GenericRow> buildValueSerde(
      final FormatInfo format,
      final PhysicalSchema schema,
      final QueryContext queryContext
  ) {
    final String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    schemas.trackValueSerdeCreation(
        loggerNamePrefix,
        schema.logicalSchema(),
        ValueFormat.of(format, schema.valueSchema().features())
    );

    return valueSerdeFactory.create(
        format,
        schema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext,
        getSerdeTracker(loggerNamePrefix)
    );
  }

  private Optional<TrackedCallback> getSerdeTracker(final String loggerNamePrefix) {
    if (System.getProperty(KSQL_TEST_TRACK_SERDE_TOPICS) == null) {
      return Optional.empty();
    }

    return Optional.of(
        (topicName, key) -> schemas.trackSerdeOp(topicName, key, loggerNamePrefix)
    );
  }

  public MaterializedFactory getMaterializedFactory() {
    return new MaterializedFactory();
  }
}
