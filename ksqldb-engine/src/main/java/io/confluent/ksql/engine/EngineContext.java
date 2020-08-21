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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.engine.rewrite.AstSanitizer;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.query.QueryExecutor;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.KafkaStreams.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the mutable state and services of the engine.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineContext {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(EngineContext.class);

  private final MutableMetaStore metaStore;
  private final ServiceContext serviceContext;
  private final CommandFactories ddlCommandFactory;
  private final DdlCommandExec ddlCommandExec;
  private final QueryIdGenerator queryIdGenerator;
  private final ProcessingLogContext processingLogContext;
  private final KsqlParser parser;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> allLiveQueries = ConcurrentHashMap.newKeySet();

  static EngineContext create(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator
  ) {
    return new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        new DefaultKsqlParser()
    );
  }

  private EngineContext(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final KsqlParser parser
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.ddlCommandFactory = new CommandFactories(serviceContext, metaStore);
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
        queryIdGenerator.createSandbox()
    );

    persistentQueries.forEach((queryId, query) ->
        sandBox.persistentQueries.put(
            query.getQueryId(),
            query.copyWith(sandBox::closeQuery)));

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

  ProcessingLogContext getProcessingLogContext() {
    return processingLogContext;
  }

  List<ParsedStatement> parse(final String sql) {
    return parser.parse(sql);
  }

  QueryIdGenerator idGenerator() {
    return queryIdGenerator;
  }

  List<QueryMetadata> getAllLiveQueries() {
    return ImmutableList.copyOf(allLiveQueries);
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
        processingLogContext
    );
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
        this::closeQuery
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
      final DdlCommand command,
      final boolean withQuery
  ) {
    final DdlCommandResult result = ddlCommandExec.execute(sqlExpression, command, withQuery);
    if (!result.isSuccess()) {
      throw new KsqlStatementException(result.getMessage(), sqlExpression);
    }
    return result.getMessage();
  }

  void registerQuery(final QueryMetadata query) {
    allLiveQueries.add(query);
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();

      // don't use persistentQueries.put(queryId) here because oldQuery.close()
      // will remove any query with oldQuery.getQueryId() from the map of persistent
      // queries
      final PersistentQueryMetadata oldQuery = persistentQueries.get(queryId);
      if (oldQuery != null) {
        oldQuery.getPhysicalPlan()
            .validateUpgrade(((PersistentQueryMetadata) query).getPhysicalPlan());

        // don't close the old query so that we don't delete the changelog
        // topics and the state store, instead use QueryMetadata#stop
        oldQuery.stop();
        unregisterQuery(oldQuery);
      }

      persistentQueries.put(queryId, persistentQuery);
      metaStore.updateForPersistentQuery(
          queryId.toString(),
          persistentQuery.getSourceNames(),
          ImmutableSet.of(persistentQuery.getSinkName()));
    }
  }

  private void closeQuery(final QueryMetadata query) {
    if (unregisterQuery(query)) {
      cleanupExternalQueryResources(query);
    }
  }

  private boolean unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      persistentQueries.remove(query.getQueryId());
      metaStore.removePersistentQuery(query.getQueryId().toString());
    }

    if (!query.getState().equals(State.NOT_RUNNING)) {
      LOG.warn(
          "Unregistering query that has not terminated. "
              + "This may happen when streams threads are hung. State: " + query.getState()
      );
    }

    return allLiveQueries.remove(query);
  }

  private void cleanupExternalQueryResources(
      final QueryMetadata query
  ) {
    final String applicationId = query.getQueryApplicationId();
    if (query.hasEverBeenStarted()) {
      SchemaRegistryUtil.cleanupInternalTopicSchemas(
          applicationId,
          serviceContext.getSchemaRegistryClient(),
          query instanceof TransientQueryMetadata);

      serviceContext.getTopicClient().deleteInternalTopics(applicationId);
    }

    StreamsErrorCollector.notifyApplicationClose(applicationId);
  }
}
