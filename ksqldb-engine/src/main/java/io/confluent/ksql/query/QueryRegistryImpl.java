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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SandboxedBinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.SandboxedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.SandboxedSharedKafkaStreamsRuntimeImpl;
import io.confluent.ksql.util.SandboxedTransientQueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.TransientQueryMetadata;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryRegistryImpl implements QueryRegistry {
  private final Logger log = LoggerFactory.getLogger(QueryRegistryImpl.class);

  private static final BiPredicate<SourceName, PersistentQueryMetadata> FILTER_QUERIES_WITH_SINK =
      (sourceName, query) -> query.getSinkName().equals(Optional.of(sourceName));

  private final Map<QueryId, PersistentQueryMetadata> persistentQueries = new ConcurrentHashMap<>();
  private final Map<QueryId, QueryMetadata> allLiveQueries = new ConcurrentHashMap<>();
  private final Map<SourceName, QueryId> createAsQueries = new ConcurrentHashMap<>();
  private final Map<SourceName, Set<QueryId>> insertQueries = new ConcurrentHashMap<>();
  private final Collection<QueryEventListener> eventListeners;
  private final QueryBuilderFactory queryBuilderFactory;
  private final MetricCollectors metricCollectors;
  private final List<SharedKafkaStreamsRuntime> streams = new ArrayList<>();
  private final List<SharedKafkaStreamsRuntime> sourceStreams = new ArrayList<>();
  private final boolean sandbox;

  public QueryRegistryImpl(
      final Collection<QueryEventListener> eventListeners,
      final MetricCollectors metricCollectors) {
    this(eventListeners, QueryBuilder::new, metricCollectors);
  }

  QueryRegistryImpl(
      final Collection<QueryEventListener> eventListeners,
      final QueryBuilderFactory queryBuilderFactory,
      final MetricCollectors metricCollectors
  ) {
    this.eventListeners = Objects.requireNonNull(eventListeners);
    this.queryBuilderFactory = Objects.requireNonNull(queryBuilderFactory);
    this.metricCollectors = metricCollectors;
    this.sandbox = false;
  }

  // Used to construct a sandbox
  private QueryRegistryImpl(final QueryRegistryImpl original) {
    queryBuilderFactory = original.queryBuilderFactory;
    original.allLiveQueries.forEach((queryId, queryMetadata) -> {
      if (queryMetadata instanceof PersistentQueryMetadataImpl) {
        final PersistentQueryMetadata sandboxed = SandboxedPersistentQueryMetadataImpl.of(
            (PersistentQueryMetadataImpl) queryMetadata,
            new ListenerImpl()
        );
        persistentQueries.put(sandboxed.getQueryId(), sandboxed);
        allLiveQueries.put(sandboxed.getQueryId(), sandboxed);
      } else if (queryMetadata instanceof BinPackedPersistentQueryMetadataImpl) {
        final PersistentQueryMetadata sandboxed = SandboxedBinPackedPersistentQueryMetadataImpl.of(
                (BinPackedPersistentQueryMetadataImpl) queryMetadata,
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
    sourceStreams.addAll(original.streams);
    this.metricCollectors = original.metricCollectors;

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
    final QueryBuilder queryBuilder = queryBuilderFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          streams,
          !sandbox);

    final TransientQueryMetadata query = queryBuilder.buildTransientQuery(
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
        new StreamsBuilder(),
        Optional.empty(),
        metricCollectors
    );
    query.initialize();
    registerTransientQuery(serviceContext, metaStore, query);
    return query;
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @Override
  public TransientQueryMetadata createStreamPullQuery(
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
      final boolean excludeTombstones,
      final ImmutableMap<TopicPartition, Long> endOffsets) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    final QueryBuilder queryBuilder = queryBuilderFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          streams,
          !sandbox);

    final TransientQueryMetadata query = queryBuilder.buildTransientQuery(
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
        new StreamsBuilder(),
        Optional.of(endOffsets),
        metricCollectors
    );
    query.initialize();
    // We don't register it as a transient query, so it won't show up in `show queries;`,
    // nor will it count against the push query limit.
    notifyCreate(serviceContext, metaStore, query);
    return query;
  }

  @Override
  public void updateStreamsPropertiesAndRestartRuntime(
      final KsqlConfig config,
      final ProcessingLogContext logContext
  ) {
    for (SharedKafkaStreamsRuntime stream : streams) {
      updateStreamsProperties(stream, config, logContext);
      stream.restartStreamsRuntime();
    }
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
      final Optional<String> sharedRuntimeId) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    final QueryBuilder queryBuilder = queryBuilderFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          streams,
          !sandbox);

    final KsqlConfig ksqlConfig = config.getConfig(true);

    final PersistentQueryMetadata query;

    final PersistentQueryMetadata oldQuery = persistentQueries.get(queryId);

    if (sharedRuntimeId.isPresent()
        && ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)
        && (oldQuery == null
        || oldQuery instanceof BinPackedPersistentQueryMetadataImpl)) {
      if (sandbox) {
        throwOnNonQueryLevelConfigs(config.getOverrides());
        streams.addAll(sourceStreams.stream()
            .filter(t -> t.getApplicationId().equals(sharedRuntimeId.get()))
            .map(SandboxedSharedKafkaStreamsRuntimeImpl::new)
            .collect(Collectors.toList()));
      }
      query = queryBuilder.buildPersistentQueryInSharedRuntime(
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
          sharedRuntimeId.get(),
          metricCollectors
      );
      query.register();
    } else {
      query = queryBuilder.buildPersistentQueryInDedicatedRuntime(
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
          new StreamsBuilder(),
          metricCollectors
      );
    }
    registerPersistentQuery(serviceContext, metaStore, query);
    return query;
  }

  private static void throwOnNonQueryLevelConfigs(final Map<String, Object> overriddenProperties) {
    final String nonQueryLevelConfigs = overriddenProperties.keySet().stream()
        .filter(s -> !PropertiesList.QueryLevelPropertyList.contains(s))
        .distinct()
        .collect(Collectors.joining(","));

    if (!nonQueryLevelConfigs.isEmpty()) {
      throw new IllegalArgumentException(String.format("When shared runtimes are enabled, the"
              + " configs %s can only be set for the entire cluster and all queries currently"
              + " running in it, and not configurable for individual queries."
              + " Please use ALTER SYSTEM to change these config for all queries.",
          nonQueryLevelConfigs));
    }
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
    closeRuntimes();
  }

  @Override
  public void closeRuntimes() {
    for (SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime : streams) {
      sharedKafkaStreamsRuntime.close();
    }
    streams.clear();
  }

  private void updateStreamsProperties(
      final SharedKafkaStreamsRuntime stream,
      final KsqlConfig config,
      final ProcessingLogContext logContext
  ) {
    final Map<String, Object> newStreamsProperties = QueryBuilder.buildStreamsProperties(
        stream.getApplicationId(),
        Optional.empty(),
        metricCollectors,
        config,
        logContext
    );
    stream.overrideStreamsProperties(newStreamsProperties);
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
      log.info("Detected that query {} already exists so will replace it."
          + "First will stop without resetting offsets", oldQuery.getQueryId());
      oldQuery.stop(true);
      unregisterQuery(oldQuery);
    }

    // If the old query was sandboxed, then the stop() won't stop the streams and will cause
    // the initialize() to fail because the stream is still running. Let's initialize the
    // query only when it is a new query or the old query is not sandboxed.
    if (oldQuery == null || !sandbox) {
      // Initialize the query before it's exposed to other threads via the map/sets.
      persistentQuery.initialize();
    }
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
    if (!query.isInitialized()) {
      throw new IllegalStateException("Transient query must be initialized before it might"
          + " be exposed to other threads via allLiveQueries");
    }
    allLiveQueries.put(query.getQueryId(), query);
    notifyCreate(serviceContext, metaStore, query);
  }

  private void unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();
      persistentQueries.remove(queryId);

      final Set<SharedKafkaStreamsRuntime> toClose = streams
          .stream()
          .filter(s -> s.getCollocatedQueries().isEmpty())
          .collect(Collectors.toSet());
      streams.removeAll(toClose);
      toClose.forEach(SharedKafkaStreamsRuntime::close);

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
  interface QueryBuilderFactory {
    QueryBuilder create(
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
