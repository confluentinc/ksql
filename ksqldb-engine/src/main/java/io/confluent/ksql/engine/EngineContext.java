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
import com.google.common.collect.Iterables;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.engine.rewrite.AstSanitizer;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.VariableSubstitutor;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.query.QueryExecutor;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SandboxedPersistentQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
  private final QueryCleanupService cleanupService;
  private final Map<SourceName, QueryId> createAsQueries = new ConcurrentHashMap();
  private final Map<SourceName, Set<QueryId>> otherQueries = new ConcurrentHashMap();

  static EngineContext create(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final QueryCleanupService cleanupService
  ) {
    return new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        new DefaultKsqlParser(),
        cleanupService
    );
  }

  private EngineContext(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final KsqlParser parser,
      final QueryCleanupService cleanupService
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.ddlCommandFactory = new CommandFactories(serviceContext, metaStore);
    this.ddlCommandExec = new DdlCommandExec(metaStore);
    this.persistentQueries = new ConcurrentHashMap<>();
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.parser = requireNonNull(parser, "parser");
    this.cleanupService = requireNonNull(cleanupService, "cleanupService");
  }

  EngineContext createSandbox(final ServiceContext serviceContext) {
    final EngineContext sandBox = EngineContext.create(
        SandboxedServiceContext.create(serviceContext),
        processingLogContext,
        metaStore.copy(),
        queryIdGenerator.createSandbox(),
        cleanupService
    );

    persistentQueries.forEach((queryId, query) ->
        sandBox.persistentQueries.put(
            query.getQueryId(),
            SandboxedPersistentQueryMetadata.of(query, sandBox::closeQuery)));

    sandBox.createAsQueries.putAll(createAsQueries);
    sandBox.otherQueries.putAll(otherQueries);

    return sandBox;
  }

  Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return Optional.ofNullable(persistentQueries.get(queryId));
  }

  Map<QueryId, PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableMap(persistentQueries);
  }

  Set<String> getQueriesWithSink(final SourceName sourceName) {
    final ImmutableSet.Builder<String> queries = ImmutableSet.builder();

    if (createAsQueries.containsKey(sourceName)) {
      queries.add(createAsQueries.get(sourceName).toString());
    }

    queries.addAll(getOtherQueriesWithSink(sourceName));
    return queries.build();
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

  private ParsedStatement substituteVariables(
      final ParsedStatement stmt,
      final Map<String, String> variablesMap
  ) {
    return (!variablesMap.isEmpty())
        ? parse(VariableSubstitutor.substitute(stmt, variablesMap)).get(0)
        : stmt ;
  }

  PreparedStatement<?> prepare(final ParsedStatement stmt, final Map<String, String> variablesMap) {
    try {
      final PreparedStatement<?> preparedStatement =
          parser.prepare(substituteVariables(stmt, variablesMap), metaStore);
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
      final SessionConfig config,
      final ServiceContext serviceContext
  ) {
    return new QueryExecutor(
        config,
        processingLogContext,
        serviceContext,
        metaStore,
        this::closeQuery
    );
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

  String executeDdl(
      final String sqlExpression,
      final DdlCommand command,
      final boolean withQuery,
      final Set<SourceName> withQuerySources
  ) {
    if (command instanceof DropSourceCommand) {
      throwIfOtherQueriesExist(((DropSourceCommand) command).getSourceName());
    }

    final DdlCommandResult result =
        ddlCommandExec.execute(sqlExpression, command, withQuery, withQuerySources);
    if (!result.isSuccess()) {
      throw new KsqlStatementException(result.getMessage(), sqlExpression);
    }

    if (command instanceof DropSourceCommand) {
      terminateCreateAsQuery(((DropSourceCommand) command).getSourceName());
    }

    return result.getMessage();
  }

  private void terminateCreateAsQuery(final SourceName sourceName) {
    createAsQueries.computeIfPresent(sourceName, (ignore , queryId) -> {
      final PersistentQueryMetadata query = persistentQueries.get(queryId);
      if (query != null) {
        query.close();
      }

      return null;
    });
  }

  private Set<String> getOtherQueriesWithSink(final SourceName sourceName) {
    final ImmutableSet.Builder<String> queries = ImmutableSet.builder();

    if (otherQueries.containsKey(sourceName)) {
      otherQueries.get(sourceName).forEach(queryId -> {
        final PersistentQueryMetadata query = persistentQueries.get(queryId);
        if (query != null && query.getSinkName().equals(sourceName)) {
          queries.add(queryId.toString());
        }
      });
    }

    return queries.build();
  }

  private Set<String> getOtherQueriesWithSource(final SourceName sourceName) {
    final ImmutableSet.Builder<String> queries = ImmutableSet.builder();

    if (otherQueries.containsKey(sourceName)) {
      otherQueries.get(sourceName).forEach(queryId -> {
        final PersistentQueryMetadata query = persistentQueries.get(queryId);
        if (query != null && query.getSourceNames().contains(sourceName)) {
          queries.add(queryId.toString());
        }
      });
    }

    return queries.build();
  }

  private void throwIfOtherQueriesExist(final SourceName sourceName) {
    final Set<String> sinkQueries = getOtherQueriesWithSink(sourceName);
    final Set<String> sourceQueries = getOtherQueriesWithSource(sourceName);

    if (!sinkQueries.isEmpty() || !sourceQueries.isEmpty()) {
      throw new KsqlReferentialIntegrityException(String.format(
          "Cannot drop %s.%n"
              + "The following queries read from this source: [%s].%n"
              + "The following queries write into this source: [%s].%n"
              + "You need to terminate them before dropping %s.",
          sourceName.text(),
          sourceQueries.stream()
                .sorted()
                .collect(Collectors.joining(", ")),
          sinkQueries.stream()
                .sorted()
                .collect(Collectors.joining(", ")),
          sourceName.text()
      ));
    }
  }

  void registerQuery(final QueryMetadata query, final boolean createAsQuery) {
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
      if (createAsQuery) {
        createAsQueries.put(persistentQuery.getSinkName(), queryId);
      } else {
        final Iterable<SourceName> allSourceNames = Iterables.concat(
            Collections.singleton(persistentQuery.getSinkName()),
            persistentQuery.getSourceNames()
        );

        allSourceNames.forEach(sourceName ->
            otherQueries.computeIfAbsent(sourceName, x -> new HashSet<>()).add(queryId));
      }
    }
  }

  private void closeQuery(final QueryMetadata query) {
    if (unregisterQuery(query)) {
      cleanupExternalQueryResources(query);
    }
  }

  private boolean unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();
      persistentQueries.remove(queryId);

      final Iterable<SourceName> allSourceNames = Iterables.concat(
          Collections.singleton(persistentQuery.getSinkName()),
          persistentQuery.getSourceNames()
      );

      // If query is a INSERT query, then this line should not cause any effect
      createAsQueries.remove(persistentQuery.getSinkName());

      // If query is a C*AS query, then these lines should not cause any effect
      allSourceNames.forEach(sourceName ->
          otherQueries.computeIfPresent(sourceName, (s, queries) -> {
            queries.remove(queryId);
            return (queries.isEmpty()) ? null : queries;
          })
      );
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
      cleanupService.addCleanupTask(
          new QueryCleanupService.QueryCleanupTask(
              serviceContext,
              applicationId,
              query instanceof TransientQueryMetadata
          ));
    }

    StreamsErrorCollector.notifyApplicationClose(applicationId);
  }

  public void close(final boolean closeQueries) {
    getAllLiveQueries().forEach(closeQueries ? QueryMetadata::close : QueryMetadata::stop);

  }
}
