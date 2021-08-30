/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueriesInSharedRuntimesImpl;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SandboxedPersistentQueriesInSharedRuntimesImpl;
import io.confluent.ksql.util.SandboxedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.SandboxedTransientQueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.ValidationSharedKafkaStreamsRuntimeImpl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;

public class QueryRegistryImpl implements QueryRegistry {
  private static final BiPredicate<SourceName, PersistentQueryMetadata> FILTER_QUERIES_WITH_SINK =
      (sourceName, query) -> query.getSinkName().equals(Optional.of(sourceName));

  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Map<QueryId, QueryMetadata> allLiveQueries;
  private final Map<SourceName, QueryId> createAsQueries;
  private final Map<SourceName, Set<QueryId>> insertQueries;
  private final Collection<QueryEventListener> eventListeners;
  private final QueryExecutorFactory executorFactory;
  private final List<SharedKafkaStreamsRuntime> streams;
  private final boolean sandbox;

  public QueryRegistryImpl(final Collection<QueryEventListener> eventListeners) {
    this(eventListeners, QueryExecutor::new);
  }

  QueryRegistryImpl(
      final Collection<QueryEventListener> eventListeners,
      final QueryExecutorFactory executorFactory
  ) {
    this.persistentQueries = new ConcurrentHashMap<>();
    this.allLiveQueries = new ConcurrentHashMap<>();
    this.createAsQueries = new ConcurrentHashMap<>();
    this.insertQueries = new ConcurrentHashMap<>();
    this.eventListeners = Objects.requireNonNull(eventListeners);
    this.executorFactory = Objects.requireNonNull(executorFactory);
    this.streams = new ArrayList<>();
    this.sandbox = false;
  }

  // Used to construct a sandbox
  private QueryRegistryImpl(final QueryRegistryImpl original) {
    executorFactory = original.executorFactory;
    persistentQueries = new ConcurrentHashMap<>();
    allLiveQueries = new ConcurrentHashMap<>();
    createAsQueries = new ConcurrentHashMap<>();
    insertQueries = new ConcurrentHashMap<>();
    original.allLiveQueries.forEach((queryId, queryMetadata) -> {
      if (queryMetadata instanceof PersistentQueryMetadataImpl) {
        final PersistentQueryMetadata sandboxed = SandboxedPersistentQueryMetadataImpl.of(
            (PersistentQueryMetadataImpl) queryMetadata,
            new ListenerImpl()
        );
        persistentQueries.put(sandboxed.getQueryId(), sandboxed);
        allLiveQueries.put(sandboxed.getQueryId(), sandboxed);
      } else if (queryMetadata instanceof PersistentQueriesInSharedRuntimesImpl) {
        final PersistentQueryMetadata sandboxed = SandboxedPersistentQueriesInSharedRuntimesImpl.of(
                (PersistentQueriesInSharedRuntimesImpl) queryMetadata,
                new ListenerImpl()
        );
        persistentQueries.put(sandboxed.getQueryId(), sandboxed);
        allLiveQueries.put(sandboxed.getQueryId(), sandboxed);
      } else {
        final TransientQueryMetadata sandboxed = SandboxedTransientQueryMetadata.of(
            (TransientQueryMetadata) queryMetadata,
            new ListenerImpl()
        );
        allLiveQueries.put(sandboxed.getQueryId(), sandboxed);
      }
    });
    createAsQueries.putAll(original.createAsQueries);
    for (final Map.Entry<SourceName, Set<QueryId>> inserts : original.insertQueries.entrySet()) {
      final Set<QueryId> sandboxInserts = Collections.synchronizedSet(new HashSet<>());
      sandboxInserts.addAll(inserts.getValue());
      insertQueries.put(inserts.getKey(), sandboxInserts);
    }
    eventListeners = original.eventListeners.stream()
        .map(QueryEventListener::createSandbox)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
    this.streams = original.streams.stream()
        .map(ValidationSharedKafkaStreamsRuntimeImpl::new)
        .collect(Collectors.toList());

    sandbox = true;
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @Override
  public TransientQueryMetadata createTransientQuery(
      final SessionConfig config,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MetaStore metaStore,
      final String statementText,
      final QueryId queryId,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<WindowInfo> windowInfo,
      final boolean excludeTombstones) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    final QueryExecutor executor = executorFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          streams,
          !sandbox);

    final TransientQueryMetadata query = executor.buildTransientQuery(
        statementText,
        queryId,
        sources,
        physicalPlan,
        planSummary,
        schema,
        limit,
        windowInfo,
        excludeTombstones,
        new ListenerImpl(),
        new StreamsBuilder()
    );
    registerTransientQuery(serviceContext, metaStore, query);
    return query;
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @Override
  public PersistentQueryMetadata createOrReplacePersistentQuery(
      final SessionConfig config,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MetaStore metaStore,
      final String statementText,
      final QueryId queryId,
      final Optional<DataSource> sinkDataSource,
      final Set<DataSource> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final boolean usesSharedRuntimes) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    final QueryExecutor executor = executorFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          streams,
          !sandbox);

    final KsqlConfig ksqlConfig = config.getConfig(true);

    final PersistentQueryMetadata query;

    if (usesSharedRuntimes) {
      query = executor.buildPersistentQueryInSharedRuntime(
          ksqlConfig,
          persistentQueryType,
          statementText,
          queryId,
          sinkDataSource,
          sources,
          physicalPlan,
          planSummary,
          new ListenerImpl(),
          () -> ImmutableList.copyOf(getPersistentQueries().values())
      );
    } else {
      query = executor.buildPersistentQueryInDedicatedRuntime(
          ksqlConfig,
          persistentQueryType,
          statementText,
          queryId,
          sinkDataSource,
          sources,
          physicalPlan,
          planSummary,
          new ListenerImpl(),
          () -> ImmutableList.copyOf(getPersistentQueries().values()),
          new StreamsBuilder()
      );
    }
    registerPersistentQuery(serviceContext, metaStore, query);
    return query;
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return Optional.ofNullable(persistentQueries.get(queryId));
  }

  @Override
  public Optional<QueryMetadata> getQuery(final QueryId queryId) {
    return Optional.ofNullable(allLiveQueries.get(queryId));
  }

  @Override
  public Map<QueryId, PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableMap(persistentQueries);
  }

  @Override
  public Set<QueryId> getQueriesWithSink(final SourceName sourceName) {
    final ImmutableSet.Builder<QueryId> queries = ImmutableSet.builder();

    if (createAsQueries.containsKey(sourceName)) {
      queries.add(createAsQueries.get(sourceName));
    }

    queries.addAll(getInsertQueries(sourceName, FILTER_QUERIES_WITH_SINK));
    return queries.build();
  }

  @Override
  public List<QueryMetadata> getAllLiveQueries() {
    return ImmutableList.copyOf(allLiveQueries.values());
  }

  @Override
  public Optional<QueryMetadata> getCreateAsQuery(final SourceName sourceName) {
    if (createAsQueries.containsKey(sourceName)) {
      return Optional.of(persistentQueries.get(createAsQueries.get(sourceName)));
    }
    return Optional.empty();
  }

  @Override
  public Set<QueryId> getInsertQueries(
      final SourceName sourceName,
      final BiPredicate<SourceName, PersistentQueryMetadata> filterQueries) {
    return insertQueries.getOrDefault(sourceName, Collections.emptySet()).stream()
        .map(persistentQueries::get)
        .filter(query -> filterQueries.test(sourceName, query))
        .map(QueryMetadata::getQueryId)
        .collect(Collectors.toSet());
  }


  @Override
  public QueryRegistry createSandbox() {
    return new QueryRegistryImpl(this);
  }

  @Override
  public void close(final boolean closePersistent) {
    for (final QueryMetadata queryMetadata : getAllLiveQueries()) {
      // only persistent queries can be stopped - transient queries must be closed (destroyed)
      if (closePersistent || queryMetadata instanceof TransientQueryMetadata) {
        queryMetadata.close();
      } else {
        // stop will not unregister the query, since it's possible for a query to be stopped
        // but still managed by the registry. So we explicitly unregister here.
        ((PersistentQueryMetadata) queryMetadata).stop();
        unregisterQuery(queryMetadata);
      }
    }
    for (SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime : streams) {
      sharedKafkaStreamsRuntime.close();
    }
  }

  private void registerPersistentQuery(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final PersistentQueryMetadata persistentQuery
  ) {
    final QueryId queryId = persistentQuery.getQueryId();

    // don't use persistentQueries.put(queryId) here because oldQuery.close()
    // will remove any query with oldQuery.getQueryId() from the map of persistent
    // queries
    final PersistentQueryMetadata oldQuery = persistentQueries.get(queryId);
    if (oldQuery != null) {
      oldQuery.getPhysicalPlan().validateUpgrade((persistentQuery).getPhysicalPlan());

      // don't close the old query so that we don't delete the changelog
      // topics and the state store, instead use QueryMetadata#stop
      oldQuery.stop();
      unregisterQuery(oldQuery);
    }

    // Initialize the query before it's exposed to other threads via the map/sets.
    persistentQuery.initialize();
    persistentQueries.put(queryId, persistentQuery);
    switch (persistentQuery.getPersistentQueryType()) {
      case CREATE_SOURCE:
        createAsQueries.put(Iterables.getOnlyElement(persistentQuery.getSourceNames()), queryId);
        break;
      case CREATE_AS:
        createAsQueries.put(persistentQuery.getSinkName().get(), queryId);
        break;
      case INSERT:
        sinkAndSources(persistentQuery).forEach(sourceName ->
            insertQueries.computeIfAbsent(sourceName,
                x -> Collections.synchronizedSet(new HashSet<>())).add(queryId));
        break;
      default:
        // do nothing
    }

    allLiveQueries.put(persistentQuery.getQueryId(), persistentQuery);
    notifyCreate(serviceContext, metaStore, persistentQuery);
  }

  private void registerTransientQuery(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final TransientQueryMetadata query
  ) {
    // Initialize the query before it's exposed to other threads via {@link allLiveQueries}.
    query.initialize();
    allLiveQueries.put(query.getQueryId(), query);
    notifyCreate(serviceContext, metaStore, query);
  }

  private void unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();
      persistentQueries.remove(queryId);

      switch (persistentQuery.getPersistentQueryType()) {
        case CREATE_SOURCE:
          createAsQueries.remove(Iterables.getOnlyElement(persistentQuery.getSourceNames()));
          break;
        case CREATE_AS:
          createAsQueries.remove(persistentQuery.getSinkName().get());
          break;
        case INSERT:
          sinkAndSources(persistentQuery).forEach(sourceName ->
              insertQueries.computeIfPresent(sourceName, (s, queries) -> {
                queries.remove(queryId);
                return (queries.isEmpty()) ? null : queries;
              })
          );
          break;
        default:
          // nothing to do with unknown query types
      }
    }

    allLiveQueries.remove(query.getQueryId());
    notifyDeregister(query);
  }

  private void notifyCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata) {
    this.eventListeners.forEach(l -> l.onCreate(serviceContext, metaStore, queryMetadata));
  }

  private void notifyDeregister(final QueryMetadata queryMetadata) {
    this.eventListeners.forEach(l -> l.onDeregister(queryMetadata));
  }

  private Iterable<SourceName> sinkAndSources(final PersistentQueryMetadata query) {
    return Iterables.concat(
        query.getSinkName().isPresent()
            ? Collections.singleton(query.getSinkName().get())
            : Collections.emptySet(),
        query.getSourceNames()
    );
  }

  @FunctionalInterface
  interface QueryExecutorFactory {
    QueryExecutor create(
        SessionConfig config,
        ProcessingLogContext processingLogContext,
        ServiceContext serviceContext,
        FunctionRegistry functionRegistry,
        List<SharedKafkaStreamsRuntime> streams,
        boolean real);
  }

  private class ListenerImpl implements QueryMetadata.Listener {
    @Override
    public void onError(final QueryMetadata queryMetadata, final QueryError error) {
      eventListeners.forEach(l -> l.onError(queryMetadata, error));
    }

    @Override
    public void onStateChange(
        final QueryMetadata queryMetadata, final State before, final State after) {
      eventListeners.forEach(l -> l.onStateChange(queryMetadata, before, after));
    }

    @Override
    public void onClose(final QueryMetadata queryMetadata) {
      unregisterQuery(queryMetadata);
      eventListeners.forEach(l -> l.onClose(queryMetadata));
    }
  }
}
