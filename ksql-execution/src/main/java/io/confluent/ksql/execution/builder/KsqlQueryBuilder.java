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

package io.confluent.ksql.execution.builder;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QuerySchemas;
import java.util.LinkedHashMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Windowed;

public final class KsqlQueryBuilder {

  private final StreamsBuilder streamsBuilder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final KeySerdeFactory keySerdeFactory;
  private final ValueSerdeFactory valueSerdeFactory;
  private final QueryId queryId;
  private final LinkedHashMap<String, PersistenceSchema> schemas = new LinkedHashMap<>();

  public static KsqlQueryBuilder of(
      StreamsBuilder streamsBuilder, KsqlConfig ksqlConfig, ServiceContext serviceContext,
      ProcessingLogContext processingLogContext, FunctionRegistry functionRegistry, QueryId queryId
  ) {
    return new KsqlQueryBuilder(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId,
        new GenericKeySerDe(),
        new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  KsqlQueryBuilder(
      StreamsBuilder streamsBuilder, KsqlConfig ksqlConfig, ServiceContext serviceContext,
      ProcessingLogContext processingLogContext, FunctionRegistry functionRegistry, QueryId queryId,
      KeySerdeFactory keySerdeFactory, ValueSerdeFactory valueSerdeFactory
  ) {
    this.streamsBuilder = requireNonNull(streamsBuilder, "streamsBuilder");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.queryId = requireNonNull(queryId, "queryId");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
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

  public QueryId getQueryId() {
    return queryId;
  }

  public KsqlQueryBuilder withKsqlConfig(KsqlConfig newConfig) {
    return of(
        streamsBuilder,
        newConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );
  }

  public QueryContext.Stacker buildNodeContext(String context) {
    return new QueryContext.Stacker()
        .push(context);
  }

  public Serde<Struct> buildKeySerde(
      FormatInfo format, PhysicalSchema schema, QueryContext queryContext
  ) {
    String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    return keySerdeFactory.create(
        format,
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext
    );
  }

  public Serde<Windowed<Struct>> buildKeySerde(
      FormatInfo format, WindowInfo window, PhysicalSchema schema, QueryContext queryContext
  ) {
    String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    return keySerdeFactory.create(
        format,
        window,
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext
    );
  }

  public Serde<GenericRow> buildValueSerde(
      FormatInfo format, PhysicalSchema schema, QueryContext queryContext
  ) {
    String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryId, queryContext);

    track(loggerNamePrefix, schema.valueSchema());

    return valueSerdeFactory.create(
        format,
        schema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext
    );
  }

  private void track(String loggerNamePrefix, PersistenceSchema schema) {
    if (schemas.containsKey(loggerNamePrefix)) {
      throw new IllegalStateException("Schema with tracked:" + loggerNamePrefix);
    }
    schemas.put(loggerNamePrefix, schema);
  }
}
