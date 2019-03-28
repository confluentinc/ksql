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

package io.confluent.ksql;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.inference.DefaultSchemaInjector;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.schema.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.DefaultTopicInjector;
import io.confluent.ksql.topic.TopicInjector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlContext {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlContext.class);

  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final Function<ServiceContext, SchemaInjector> schemaInjectorFactory;
  private final Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory;

  /**
   * Create a KSQL context object with the given properties. A KSQL context has it's own metastore
   * valid during the life of the object.
   */
  public static KsqlContext create(
      final KsqlConfig ksqlConfig,
      final ProcessingLogContext processingLogContext
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig cannot be null.");
    final ServiceContext serviceContext = DefaultServiceContext.create(ksqlConfig);
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    UdfLoader.newInstance(ksqlConfig, functionRegistry, ".").load();
    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final KsqlEngine engine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        serviceId);

    return new KsqlContext(
        serviceContext,
        ksqlConfig,
        engine,
        sc -> new DefaultSchemaInjector(
            new SchemaRegistryTopicSchemaSupplier(sc.getSchemaRegistryClient())),
        DefaultTopicInjector::new);
  }

  @VisibleForTesting
  KsqlContext(
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final Function<ServiceContext, SchemaInjector> schemaInjectorFactory,
      final Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.schemaInjectorFactory = Objects
        .requireNonNull(schemaInjectorFactory, "schemaInjectorFactory");
    this.topicInjectorFactory = Objects
        .requireNonNull(topicInjectorFactory, "topicInjectorFactory");
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  public MetaStore getMetaStore() {
    return ksqlEngine.getMetaStore();
  }

  /**
   * Execute the ksql statement in this context.
   */
  public List<QueryMetadata> sql(final String sql) {
    return sql(sql, Collections.emptyMap());
  }

  public List<QueryMetadata> sql(final String sql, final Map<String, Object> overriddenProperties) {
    final List<ParsedStatement> statements = ksqlEngine.parse(sql);

    final KsqlExecutionContext sandbox = ksqlEngine.createSandbox();
    final SchemaInjector sandboxSchemaInjector = schemaInjectorFactory
        .apply(sandbox.getServiceContext());
    final TopicInjector sandboxTopicInjector = topicInjectorFactory.apply(sandbox);

    for (ParsedStatement stmt : statements) {
      execute(
          sandbox,
          stmt,
          ksqlConfig,
          overriddenProperties,
          sandboxSchemaInjector,
          sandboxTopicInjector);
    }

    final SchemaInjector schemaInjector = schemaInjectorFactory.apply(serviceContext);
    final TopicInjector topicInjector = topicInjectorFactory.apply(ksqlEngine);
    final List<QueryMetadata> queries = new ArrayList<>();
    for (final ParsedStatement parsed : statements) {
      execute(ksqlEngine, parsed, ksqlConfig, overriddenProperties, schemaInjector, topicInjector)
          .getQuery()
          .ifPresent(queries::add);
    }

    for (final QueryMetadata queryMetadata : queries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        queryMetadata.start();
      } else {
        LOG.warn("Ignoring statemenst: {}", sql);
        LOG.warn("Only CREATE statements can run in KSQL embedded mode.");
      }
    }

    return queries;
  }

  /**
   * @deprecated use {@link #getPersistentQueries}.
   */
  @Deprecated
  public Set<QueryMetadata> getRunningQueries() {
    return new HashSet<>(ksqlEngine.getPersistentQueries());
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ksqlEngine.getPersistentQueries();
  }

  public void close() {
    ksqlEngine.close();
    serviceContext.close();
  }

  public void terminateQuery(final QueryId queryId) {
    ksqlEngine.getPersistentQuery(queryId).ifPresent(QueryMetadata::close);
  }

  private ExecuteResult execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final SchemaInjector schemaInjector,
      final TopicInjector topicInjector) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final PreparedStatement<?> withSchema = schemaInjector.forStatement(prepared);
    final PreparedStatement<?> withInferredTopic =
        topicInjector.forStatement(withSchema, ksqlConfig, overriddenProperties);
    return executionContext.execute(withInferredTopic, ksqlConfig, overriddenProperties);
  }
}
