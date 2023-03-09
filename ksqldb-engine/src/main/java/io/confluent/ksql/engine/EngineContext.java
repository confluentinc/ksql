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

package io.confluent.ksql.engine;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.engine.rewrite.AstSanitizer;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.VariableSubstitutor;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.query.KafkaStreamsQueryValidator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.query.QueryRegistryImpl;
import io.confluent.ksql.query.QueryValidator;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Holds the mutable state and services of the engine.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineContext {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final BiPredicate<SourceName, PersistentQueryMetadata> FILTER_QUERIES_WITH_SINK =
      (sourceName, query) -> query.getSinkName().equals(Optional.of(sourceName));

  private static final BiPredicate<SourceName, PersistentQueryMetadata> FILTER_QUERIES_WITH_SOURCE =
      (sourceName, query) -> query.getSourceNames().contains(sourceName);

  private final MutableMetaStore metaStore;
  private final ServiceContext serviceContext;
  private final CommandFactories ddlCommandFactory;
  private final DdlCommandExec ddlCommandExec;
  private final QueryIdGenerator queryIdGenerator;
  private final ProcessingLogContext processingLogContext;
  private final KsqlParser parser;
  private final QueryCleanupService cleanupService;
  private final QueryRegistry queryRegistry;
  private final RuntimeAssignor runtimeAssignor;
  private KsqlConfig ksqlConfig;

  static EngineContext create(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final QueryCleanupService cleanupService,
      final KsqlConfig ksqlConfig,
      final Collection<QueryEventListener> registrationListeners,
      final MetricCollectors metricCollectors
  ) {
    return new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        new DefaultKsqlParser(),
        cleanupService,
        ksqlConfig,
        new QueryRegistryImpl(registrationListeners, metricCollectors),
        new RuntimeAssignor(ksqlConfig)
    );
  }

  private EngineContext(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final KsqlParser parser,
      final QueryCleanupService cleanupService,
      final KsqlConfig ksqlConfig,
      final QueryRegistry queryRegistry,
      final RuntimeAssignor runtimeAssignor
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.ddlCommandFactory = new CommandFactories(serviceContext, metaStore);
    this.ddlCommandExec = new DdlCommandExec(metaStore);
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.parser = requireNonNull(parser, "parser");
    this.cleanupService = requireNonNull(cleanupService, "cleanupService");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.queryRegistry = requireNonNull(queryRegistry, "queryRegistry");
    this.runtimeAssignor = requireNonNull(runtimeAssignor, "runtimeAssignor");
  }

  synchronized EngineContext createSandbox(final ServiceContext serviceContext) {
    this.runtimeAssignor.rebuildAssignment(queryRegistry.getPersistentQueries().values());
    return new EngineContext(
        SandboxedServiceContext.create(serviceContext),
        processingLogContext,
        metaStore.copy(),
        queryIdGenerator.createSandbox(),
        new DefaultKsqlParser(),
        cleanupService,
        ksqlConfig,
        queryRegistry.createSandbox(),
        runtimeAssignor.createSandbox()
    );
  }

  MutableMetaStore getMetaStore() {
    return metaStore;
  }

  ServiceContext getServiceContext() {
    return serviceContext;
  }

  ProcessingLogContext getProcessingLogContext() {
    return processingLogContext;
  }

  List<ParsedStatement> parse(final String sql) {
    return parser.parse(sql);
  }

  QueryIdGenerator idGenerator() {
    return queryIdGenerator;
  }

  QueryRegistry getQueryRegistry() {
    return queryRegistry;
  }

  RuntimeAssignor getRuntimeAssignor() {
    return runtimeAssignor;
  }

  synchronized KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  synchronized void alterSystemProperty(final Map<String, String> overrides) {
    this.ksqlConfig = this.ksqlConfig.cloneWithPropertyOverwrite(overrides);
  }

  private ParsedStatement substituteVariables(
      final ParsedStatement stmt,
      final Map<String, String> variablesMap
  ) {
    return (!variablesMap.isEmpty())
        ? parse(VariableSubstitutor.substitute(stmt, variablesMap)).get(0)
        : stmt ;
  }

  synchronized PreparedStatement<?> prepare(final ParsedStatement stmt,
      final Map<String, String> variablesMap) {
    try {
      final PreparedStatement<?> preparedStatement =
          parser.prepare(substituteVariables(stmt, variablesMap), metaStore);
      return PreparedStatement.of(
          preparedStatement.getUnMaskedStatementText(),
          AstSanitizer.sanitize(
              preparedStatement.getStatement(),
              metaStore,
              ksqlConfig.getBoolean(KsqlConfig.KSQL_LAMBDAS_ENABLED),
              ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)
          ));
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Exception while preparing statement: " + e.getMessage(), stmt.getMaskedStatementText(),
          e);
    }
  }

  QueryEngine createQueryEngine(final ServiceContext serviceContext) {
    return new QueryEngine(
        serviceContext,
        processingLogContext
    );
  }

  QueryValidator createQueryValidator() {
    return new KafkaStreamsQueryValidator();
  }

  DdlCommand createDdlCommand(
      final String sqlExpression,
      final ExecutableDdlStatement statement,
      final SessionConfig config
  ) {
    return ddlCommandFactory.create(
        sqlExpression,
        statement,
        config
    );
  }

  DdlCommand createDdlCommand(final KsqlStructuredDataOutputNode outputNode) {
    return ddlCommandFactory.create(outputNode);
  }

  String executeDdl(
      final String sqlExpression,
      final DdlCommand command,
      final boolean withQuery,
      final Set<SourceName> withQuerySources,
      final boolean restoreInProgress
  ) {
    if (command instanceof DropSourceCommand && !restoreInProgress) {
      throwIfInsertQueriesExist(((DropSourceCommand) command).getSourceName());
    }

    final DdlCommandResult result =
        ddlCommandExec.execute(sqlExpression, command, withQuery, withQuerySources,
            restoreInProgress);
    if (!result.isSuccess()) {
      throw new KsqlStatementException(result.getMessage(), sqlExpression);
    }

    if (command instanceof DropSourceCommand) {
      // terminate the query (linked by create_as commands) after deleting the source to avoid
      // other commands to create queries from this source while the query is being terminated
      maybeTerminateCreateAsQuery(((DropSourceCommand) command).getSourceName());
    }

    return result.getMessage();
  }

  private void maybeTerminateCreateAsQuery(final SourceName sourceName) {
    queryRegistry.getCreateAsQuery(sourceName).ifPresent(t -> {
      t.close();
      if (t instanceof BinPackedPersistentQueryMetadataImpl) {
        runtimeAssignor.dropQuery((BinPackedPersistentQueryMetadataImpl) t);
      }
    });
  }

  private void throwIfInsertQueriesExist(final SourceName sourceName) {
    final Set<QueryId> sinkQueries
        = queryRegistry.getInsertQueries(sourceName, FILTER_QUERIES_WITH_SINK);
    final Set<QueryId> sourceQueries
        = queryRegistry.getInsertQueries(sourceName, FILTER_QUERIES_WITH_SOURCE);

    if (!sinkQueries.isEmpty() || !sourceQueries.isEmpty()) {
      throw new KsqlReferentialIntegrityException(String.format(
          "Cannot drop %s.%n"
              + "The following queries read from this source: [%s].%n"
              + "The following queries write into this source: [%s].%n"
              + "You need to terminate them before dropping %s.",
          sourceName.text(),
          sourceQueries.stream()
              .map(QueryId::toString)
              .sorted()
              .collect(Collectors.joining(", ")),
          sinkQueries.stream()
              .map(QueryId::toString)
              .sorted()
              .collect(Collectors.joining(", ")),
          sourceName.text()
      ));
    }
  }
}
