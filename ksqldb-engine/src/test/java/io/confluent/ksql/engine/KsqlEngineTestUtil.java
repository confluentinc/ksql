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

package io.confluent.ksql.engine;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.schema.ksql.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.ksql.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.SourcePropertyInjector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class KsqlEngineTestUtil {

  private KsqlEngineTestUtil() {
  }

  public static KsqlEngine createKsqlEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore,
      final MetricCollectors metricCollectors
  ) {
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        (engine) -> new KsqlEngineMetrics("", engine, Collections.emptyMap(), Optional.empty(), metricCollectors),
        new SequentialQueryIdGenerator(),
        new KsqlConfig(Collections.emptyMap()),
        Collections.emptyList(),
        metricCollectors
    );
  }

  public static KsqlEngine createKsqlEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore,
      final KsqlConfig ksqlConfig
  ) {
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        (engine) -> new KsqlEngineMetrics("", engine, Collections.emptyMap(), Optional.empty(), new MetricCollectors()),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        Collections.emptyList(),
        new MetricCollectors()
    );
  }

  public static KsqlEngine createKsqlEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator,
      final KsqlConfig ksqlConfig,
      final MetricCollectors metricCollectors
  ) {
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        engineMetricsFactory,
        queryIdGenerator,
        ksqlConfig,
        Collections.emptyList(),
        metricCollectors
    );
  }

  public static List<QueryMetadata> execute(
      final ServiceContext serviceContext,
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return execute(serviceContext, engine, sql, ksqlConfig, overriddenProperties, Optional.empty());
  }

  public static TransientQueryMetadata executeQuery(
      final ServiceContext serviceContext,
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties) {
    final ParsedStatement stmt = engine.parse(sql).get(0);
    final PreparedStatement<?> prepared = engine.prepare(stmt);
    final ConfiguredStatement<Query> configured = ConfiguredStatement.of(
        prepared, SessionConfig.of(ksqlConfig, overriddenProperties)).cast();
    try {
      return engine.executeTransientQuery(serviceContext, configured, false);
    } catch (final KsqlStatementException e) {
      // use the original statement text in the exception so that tests
      // can easily check that the failed statement is the input statement
      throw new KsqlStatementException(e.getRawMessage(), stmt.getMaskedStatementText(),
          e.getCause());
    }
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  public static List<QueryMetadata> execute(
      final ServiceContext serviceContext,
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(supplier -> new DefaultSchemaInjector(supplier, engine, serviceContext));

    return statements.stream()
        .map(stmt ->
            execute(serviceContext, engine, stmt, ksqlConfig, overriddenProperties, schemaInjector))
        .map(ExecuteResult::getQuery)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  private static ExecuteResult execute(
      final ServiceContext serviceContext,
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<DefaultSchemaInjector> schemaInjector
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(
        prepared, SessionConfig.of(ksqlConfig, overriddenProperties));
    final ConfiguredStatement<?> withFormats = new DefaultFormatInjector().inject(configured);
    final ConfiguredStatement<?> withSourceProps = new SourcePropertyInjector().inject(withFormats);
    final ConfiguredStatement<?> withSchema =
        schemaInjector
            .map(injector -> injector.inject(withSourceProps))
            .orElse((ConfiguredStatement) withSourceProps);
    try {
      return executionContext.execute(serviceContext, withSchema);
    } catch (final KsqlStatementException e) {
      // use the original statement text in the exception so that tests
      // can easily check that the failed statement is the input statement
      throw new KsqlStatementException(e.getRawMessage(), stmt.getMaskedStatementText(),
          e.getCause());
    }
  }
}
