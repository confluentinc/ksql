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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.engine.rewrite.AstSanitizer;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.query.QueryExecutor;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Holds the mutable state and services of the engine.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineContext {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final MutableMetaStore metaStore;
  private final ServiceContext serviceContext;
  private final CommandFactories ddlCommandFactory;
  private final DdlCommandExec ddlCommandExec;
  private final QueryIdGenerator queryIdGenerator;
  private final ProcessingLogContext processingLogContext;
  private final KsqlParser parser;
  private final BiConsumer<ServiceContext, QueryMetadata> outerOnQueryCloseCallback;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;

  static EngineContext create(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final BiConsumer<ServiceContext, QueryMetadata> onQueryCloseCallback
  ) {
    return new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        onQueryCloseCallback,
        new DefaultKsqlParser()
    );
  }

  private EngineContext(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final BiConsumer<ServiceContext, QueryMetadata> onQueryCloseCallback,
      final KsqlParser parser
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.ddlCommandFactory = new CommandFactories(serviceContext, metaStore);
    this.outerOnQueryCloseCallback = requireNonNull(onQueryCloseCallback, "onQueryCloseCallback");
    this.ddlCommandExec = new DdlCommandExec(metaStore);
    this.persistentQueries = new ConcurrentHashMap<>();
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.parser = requireNonNull(parser, "parser");
  }

  EngineContext createSandbox(final ServiceContext serviceContext) {
    final EngineContext sandBox = EngineContext.create(
        SandboxedServiceContext.create(serviceContext),
        processingLogContext,
        metaStore.copy(),
        queryIdGenerator.createSandbox(),
        (sc, query) -> { /* No-op */ }
    );

    persistentQueries.forEach((queryId, query) ->
        sandBox.persistentQueries.put(
            query.getQueryId(),
            query.copyWith(sandBox::unregisterQuery)));

    return sandBox;
  }

  Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return Optional.ofNullable(persistentQueries.get(queryId));
  }

  Map<QueryId, PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableMap(persistentQueries);
  }

  MutableMetaStore getMetaStore() {
    return metaStore;
  }

  ServiceContext getServiceContext() {
    return serviceContext;
  }

  List<ParsedStatement> parse(final String sql) {
    return parser.parse(sql);
  }

  PreparedStatement<?> prepare(final ParsedStatement stmt) {
    try {
      final PreparedStatement<?> preparedStatement = parser.prepare(stmt, metaStore);
      return PreparedStatement.of(
          preparedStatement.getStatementText(),
          AstSanitizer.sanitize(preparedStatement.getStatement(), metaStore)
      );
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Exception while preparing statement: " + e.getMessage(), stmt.getStatementText(), e);
    }
  }

  QueryEngine createQueryEngine(final ServiceContext serviceContext) {
    return new QueryEngine(
        serviceContext,
        processingLogContext,
        queryIdGenerator);
  }

  QueryExecutor createQueryExecutor(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final ServiceContext serviceContext) {
    return new QueryExecutor(
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties),
        overriddenProperties,
        processingLogContext,
        serviceContext,
        metaStore,
        this::unregisterQuery
    );
  }

  DdlCommand createDdlCommand(
      final String sqlExpression,
      final ExecutableDdlStatement statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return ddlCommandFactory.create(
        sqlExpression,
        statement,
        ksqlConfig,
        overriddenProperties
    );
  }

  String executeDdl(
      final String sqlExpression,
      final DdlCommand command
  ) {
    final DdlCommandResult result = ddlCommandExec.execute(sqlExpression, command);
    if (!result.isSuccess()) {
      throw new KsqlStatementException(result.getMessage(), sqlExpression);
    }
    return result.getMessage();
  }

  void registerQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();

      if (persistentQueries.putIfAbsent(queryId, persistentQuery) != null) {
        throw new IllegalStateException("Query already registered:" + queryId);
      }

      metaStore.updateForPersistentQuery(
          queryId.getId(),
          persistentQuery.getSourceNames(),
          ImmutableSet.of(persistentQuery.getSinkName()));
    }
  }

  private void unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      persistentQueries.remove(persistentQuery.getQueryId());
      metaStore.removePersistentQuery(persistentQuery.getQueryId().getId());
    }

    outerOnQueryCloseCallback.accept(serviceContext, query);
  }
}
