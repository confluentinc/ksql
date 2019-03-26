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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import java.util.LinkedHashMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;

public final class KsqlQueryBuilder {

  private final StreamsBuilder streamsBuilder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final QueryId queryId;
  private final LinkedHashMap<String, Schema> schemas = new LinkedHashMap<>();

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
        queryId
    );
  }

  private KsqlQueryBuilder(
      final StreamsBuilder streamsBuilder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryId queryId
  ) {
    this.streamsBuilder = requireNonNull(streamsBuilder, "streamsBuilder");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.queryId = requireNonNull(queryId, "queryId");
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

  public Serde<GenericRow> buildGenericRowSerde(
      final KsqlTopicSerDe topicSerDe,
      final Schema schema,
      final QueryContext queryContext
  ) {
    final String loggerNamePrefix = QueryLoggerUtil.queryLoggerName(queryContext);

    track(loggerNamePrefix, schema);

    return topicSerDe.getGenericRowSerde(
        schema,
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        loggerNamePrefix,
        processingLogContext
    );
  }

  private void track(final String loggerNamePrefix, final Schema schema) {
    if (schemas.containsKey(loggerNamePrefix)) {
      throw new IllegalStateException("Schema with tracked:" + loggerNamePrefix);
    }
    schemas.put(loggerNamePrefix, schema);
  }
}
