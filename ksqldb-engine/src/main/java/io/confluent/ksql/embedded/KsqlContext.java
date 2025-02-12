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

package io.confluent.ksql.embedded;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.PropertyOverrider;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlContext implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlContext.class);

  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;

  /**
   * Create a KSQL context object with the given properties. A KSQL context has it's own metastore
   * valid during the life of the object.
   */
  public static KsqlContext create(
      final KsqlConfig ksqlConfig,
      final ProcessingLogContext processingLogContext,
      final MetricCollectors metricCollectors
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig cannot be null.");
    final ServiceContext serviceContext =
        ServiceContextFactory.create(ksqlConfig, DisabledKsqlClient::instance);
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    UserFunctionLoader.newInstance(
        ksqlConfig,
        functionRegistry,
        ".",
        metricCollectors.getMetrics()
    ).load();
    final ServiceInfo serviceInfo = ServiceInfo.create(ksqlConfig);
    final KsqlEngine engine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        serviceInfo,
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        Collections.emptyList(),
        metricCollectors
    );

    return new KsqlContext(
        serviceContext,
        ksqlConfig,
        engine,
        Injectors.DEFAULT
    );
  }

  @VisibleForTesting
  KsqlContext(
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
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

  public List<QueryMetadata> sql(final String sql, final Map<String, ?> overriddenProperties) {
    final List<ParsedStatement> statements = ksqlEngine.parse(sql);

    final KsqlExecutionContext sandbox = ksqlEngine.createSandbox(ksqlEngine.getServiceContext());
    final Map<String, Object> validationOverrides = new HashMap<>(overriddenProperties);
    for (final ParsedStatement stmt : statements) {
      execute(
          sandbox,
          stmt,
          ksqlConfig,
          validationOverrides,
          injectorFactory.apply(sandbox, sandbox.getServiceContext()));
    }

    final List<QueryMetadata> queries = new ArrayList<>();
    final Injector injector = injectorFactory.apply(ksqlEngine, serviceContext);
    final Map<String, Object> executionOverrides = new HashMap<>(overriddenProperties);
    for (final ParsedStatement parsed : statements) {
      execute(ksqlEngine, parsed, ksqlConfig, executionOverrides, injector)
          .getQuery()
          .ifPresent(queries::add);
    }

    for (final QueryMetadata queryMetadata : queries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        queryMetadata.start();
      } else {
        QueryLogger.warn("Ignoring statement", sql);
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

  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return ksqlEngine.getPersistentQuery(queryId);
  }

  public void close() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @VisibleForTesting
  public void terminateQuery(final QueryId queryId) {
    ksqlEngine.getPersistentQuery(queryId).ifPresent(t -> {
      t.close();
      ksqlEngine.removeQueryFromAssignor(t);
    });
  }

  @VisibleForTesting
  public void pauseQuery(final QueryId queryId) {
    ksqlEngine.getPersistentQuery(queryId).ifPresent(QueryMetadata::pause);
  }

  @VisibleForTesting
  public void resumeQuery(final QueryId queryId) {
    ksqlEngine.getPersistentQuery(queryId).ifPresent(QueryMetadata::resume);
  }

  private static ExecuteResult execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> mutableSessionPropertyOverrides,
      final Injector injector
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);

    final ConfiguredStatement<?> configured = injector.inject(ConfiguredStatement.of(
        prepared,
        SessionConfig.of(ksqlConfig, mutableSessionPropertyOverrides)
    ));

    final CustomExecutor executor =
        CustomExecutors.EXECUTOR_MAP.getOrDefault(
            configured.getStatement().getClass(),
            (passedExecutionContext, s, props) -> passedExecutionContext.execute(
                passedExecutionContext.getServiceContext(), s));

    return executor.apply(
        executionContext,
        configured,
        mutableSessionPropertyOverrides
    );
  }

  @FunctionalInterface
  private interface CustomExecutor {
    ExecuteResult apply(
        KsqlExecutionContext executionContext,
        ConfiguredStatement<?> statement,
        Map<String, Object> mutableSessionPropertyOverrides
    );
  }

  @SuppressWarnings("unchecked")
  private enum CustomExecutors {

    SET_PROPERTY(SetProperty.class, (executionContext, stmt, props) -> {
      PropertyOverrider.set((ConfiguredStatement<SetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    }),
    UNSET_PROPERTY(UnsetProperty.class, (executionContext, stmt, props) -> {
      PropertyOverrider.unset((ConfiguredStatement<UnsetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    }),
    QUERY(Query.class, (executionContext, stmt, props) -> {
      return ExecuteResult.of(
          executionContext.executeTransientQuery(
              executionContext.getServiceContext(),
              stmt.cast(),
              false
          )
      );
    })
    ;

    public static final Map<Class<? extends Statement>, CustomExecutor> EXECUTOR_MAP =
        ImmutableMap.copyOf(
            EnumSet.allOf(CustomExecutors.class)
                .stream()
                .collect(Collectors.toMap(
                    CustomExecutors::getStatementClass,
                    CustomExecutors::getExecutor))
        );

    private final Class<? extends Statement> statementClass;
    private final CustomExecutor executor;

    CustomExecutors(
        final Class<? extends Statement> statementClass,
        final CustomExecutor executor) {
      this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
      this.executor = Objects.requireNonNull(executor, "executor");
    }

    private Class<? extends Statement> getStatementClass() {
      return statementClass;
    }

    private CustomExecutor getExecutor() {
      return this::execute;
    }

    public ExecuteResult execute(
        final KsqlExecutionContext executionContext,
        final ConfiguredStatement<?> statement,
        final Map<String, Object> mutableSessionPropertyOverrides
    ) {
      return executor.apply(
          executionContext, statement, mutableSessionPropertyOverrides);
    }
  }
}
