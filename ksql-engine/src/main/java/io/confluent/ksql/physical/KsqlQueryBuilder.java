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

package io.confluent.ksql.physical;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.serde.SerdeFactory;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public final class KsqlQueryBuilder {

  private final StreamsBuilder streamsBuilder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final SerdeFactories serdeFactories;
  private final QueryId queryId;
  private final LinkedHashMap<String, PersistenceSchema> schemas = new LinkedHashMap<>();

  public static KsqlQueryBuilder of(
      final StreamsBuilder streamsBuilder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryId queryId
  ) {
    return new KsqlQueryBuilder(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId,
        new KsqlSerdeFactories()
    );
  }

  @VisibleForTesting
  KsqlQueryBuilder(
      final StreamsBuilder streamsBuilder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryId queryId,
      final SerdeFactories serdeFactories
  ) {
    this.streamsBuilder = requireNonNull(streamsBuilder, "streamsBuilder");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.queryId = requireNonNull(queryId, "queryId");
    this.serdeFactories = requireNonNull(serdeFactories, "serdeFactories");
  }

  public ProcessingLogContext getProcessingLogContext() {
    return processingLogContext;
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public StreamsBuilder getStreamsBuilder() {
    return streamsBuilder;
  }

  public QuerySchemas getSchemas() {
    return QuerySchemas.of(schemas);
  }

  public KsqlQueryBuilder withKsqlConfig(final KsqlConfig newConfig) {
    return of(
        streamsBuilder,
        newConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );
  }

  public QueryContext.Stacker buildNodeContext(final PlanNodeId id) {
    return new QueryContext.Stacker(queryId)
        .push(id.toString());
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent", "MethodMayBeStatic"})
  public SerdeFactory<?> buildKeySerde(final KeyFormat keyFormat) {
    final Optional<WindowType> windowType = keyFormat.getWindowType();
    final Optional<Long> windowSize = keyFormat.getWindowSize()
        .map(Duration::toMillis);

    if (!windowType.isPresent()) {
      return (SerdeFactory) Serdes::String;
    }

    if (windowType.get() == WindowType.SESSION) {
      return () -> (Serde) WindowedSerdes.sessionWindowedSerdeFrom(String.class);
    }

    return () -> (Serde) WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.get());
  }

  public Serde<GenericRow> buildGenericRowSerde(
      final ValueFormat valueFormat,
      final PhysicalSchema schema,
      final QueryContext queryContext
  ) {
    final String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryContext);

    track(loggerNamePrefix, schema.valueSchema());

    final KsqlSerdeFactory valueSerdeFactory = serdeFactories.create(
        valueFormat.getFormatInfo().getFormat(),
        valueFormat.getFormatInfo().getAvroFullSchemaName()
    );

    return GenericRowSerDe.from(
        valueSerdeFactory,
        schema,
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext
    );
  }

  private void track(final String loggerNamePrefix, final PersistenceSchema schema) {
    if (schemas.containsKey(loggerNamePrefix)) {
      throw new IllegalStateException("Schema with tracked:" + loggerNamePrefix);
    }
    schemas.put(loggerNamePrefix, schema);
  }
}
