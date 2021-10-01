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

import static io.confluent.ksql.query.QueryBuilder.buildConfiguredClassifiers;
import static io.confluent.ksql.util.QueryApplicationId.getSandboxAppIdForOriginalRuntimeAppId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.metrics.RocksDBMetricsCollector;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.StorageUtilizationMetricsReporter;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryApplicationId;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SandboxedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.SandboxedSharedKafkaStreamsRuntimeImpl;
import io.confluent.ksql.util.SandboxedSharedRuntimePersistentQueryMetadata;
import io.confluent.ksql.util.SandboxedTransientQueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.SharedKafkaStreamsRuntimeImpl;
import io.confluent.ksql.util.SharedRuntimePersistentQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
  private final KsqlConfig config;
  private final ProcessingLogContext processingLogContext;
  private final QueryBuilderFactory queryBuilderFactory;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Map<String, SharedKafkaStreamsRuntime> sharedRuntimesByAppId = new HashMap<>();
  private final int numInitialSharedRuntimes;
  private final boolean sandbox;

  public QueryRegistryImpl(
      final Collection<QueryEventListener> eventListeners,
      final KsqlConfig config,
      final ProcessingLogContext processingLogContext,
      final KafkaStreamsBuilder kafkaStreamsBuilder
  ) {
    this(eventListeners, config, processingLogContext, kafkaStreamsBuilder, QueryBuilder::new);
  }

  QueryRegistryImpl(
      final Collection<QueryEventListener> eventListeners,
      final KsqlConfig config,
      final ProcessingLogContext processingLogContext,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final QueryBuilderFactory queryBuilderFactory
  ) {
    this.eventListeners = Objects.requireNonNull(eventListeners);
    this.queryBuilderFactory = Objects.requireNonNull(queryBuilderFactory);
    this.config = Objects.requireNonNull(config, "config");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.kafkaStreamsBuilder = kafkaStreamsBuilder;
    this.numInitialSharedRuntimes = config.getInt(KsqlConfig.KSQL_NUM_INITIAL_SHARED_RUNTIMES);
    this.sandbox = false;

    for (int i = 0; i < numInitialSharedRuntimes; ++i) {
      constructSharedKafkaStreamsRuntime(i);
    }
  }

  // Used to construct a sandbox
  private QueryRegistryImpl(final QueryRegistryImpl original) {
    this.config = original.config;
    this.processingLogContext = original.processingLogContext;
    this.queryBuilderFactory = original.queryBuilderFactory;
    this.kafkaStreamsBuilder = original.kafkaStreamsBuilder;
    this.numInitialSharedRuntimes = original.numInitialSharedRuntimes;
    sandbox = true;

    final Collection<SharedKafkaStreamsRuntime> originalRuntimes
        = original.sharedRuntimesByAppId.values();
    for (final SharedKafkaStreamsRuntime originalRuntime : originalRuntimes) {
      final Map<String, Object> streamsProperties =
          new ConcurrentHashMap<>(originalRuntime.getStreamProperties());
      final String sandboxRuntimeAppId =
          getSandboxAppIdForOriginalRuntimeAppId(originalRuntime.getApplicationId());
      streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, sandboxRuntimeAppId);
      final String sandboxStateDir =
          originalRuntime.getStreamProperties().getOrDefault(
              StreamsConfig.STATE_DIR_CONFIG,
              "/tmp") + "/sandbox";
      streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, sandboxStateDir);

      final SharedKafkaStreamsRuntime sandboxRuntime = new SandboxedSharedKafkaStreamsRuntimeImpl(
          originalRuntime,
          kafkaStreamsBuilder,
          streamsProperties
      );
      sharedRuntimesByAppId.put(sandboxRuntimeAppId, sandboxRuntime);
    }

    original.allLiveQueries.forEach((queryId, queryMetadata) -> {
      if (queryMetadata instanceof PersistentQueryMetadataImpl) {
        final PersistentQueryMetadata sandboxed = SandboxedPersistentQueryMetadataImpl.of(
            (PersistentQueryMetadataImpl) queryMetadata,
            new ListenerImpl()
        );
        persistentQueries.put(sandboxed.getQueryId(), sandboxed);
        allLiveQueries.put(sandboxed.getQueryId(), sandboxed);
      } else if (queryMetadata instanceof SharedRuntimePersistentQueryMetadata) {
        final PersistentQueryMetadata sandboxed =
            SandboxedSharedRuntimePersistentQueryMetadata
                .of((SharedRuntimePersistentQueryMetadata) queryMetadata);
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
          kafkaStreamsBuilder);

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
        Optional.empty()
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
        Optional.of(endOffsets)
    );
    query.initialize();
    // We don't register it as a transient query, so it won't show up in `show queries;`,
    // nor will it count against the push query limit.
    notifyCreate(serviceContext, metaStore, query);
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
      final Optional<String> runtimeId) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    final QueryBuilder queryBuilder = queryBuilderFactory.create(
          config,
          processingLogContext,
          serviceContext,
          metaStore,
          kafkaStreamsBuilder);

    final KsqlConfig ksqlConfig = config.getConfig(true);

    final PersistentQueryMetadata query;

    if (runtimeId.isPresent()) {
      final SharedKafkaStreamsRuntime queryRuntime = sandbox
          ? sharedRuntimesByAppId.get(getSandboxAppIdForOriginalRuntimeAppId(runtimeId.get()))
          : sharedRuntimesByAppId.get(runtimeId.get());
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
          queryRuntime,
          () -> ImmutableList.copyOf(getPersistentQueries().values())
      );
      queryRuntime.register((SharedRuntimePersistentQueryMetadata) query);
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
          new StreamsBuilder()
      );
    }
    registerPersistentQuery(serviceContext, metaStore, query);
    return query;
  }

  @Override
  public String getSharedRuntimeIdForQuery(final QueryId queryId, final Set<SourceName> sources) {
    if (persistentQueries.containsKey(queryId)) {
      return persistentQueries.get(queryId).getQueryApplicationId();
    }
    // Get the least-loaded runtime that can accept this query based on constraints
    final List<SharedKafkaStreamsRuntime> sharedRuntimesByQueryLoad =
        new ArrayList<>(sharedRuntimesByAppId.values());
    sharedRuntimesByQueryLoad.sort(
        Comparator.comparingInt(SharedKafkaStreamsRuntime::numberOfQueries));
    for (final SharedKafkaStreamsRuntime sharedRuntime : sharedRuntimesByQueryLoad) {
      if (sharedRuntime.getQueries().contains(queryId)
          || sharedRuntime.getSources().stream().noneMatch(sources::contains)) {
        // Need to mark the sources right away since we've essentially reserved a spot in the
        // runtime for this query, even if it hasn't actually been added to it yet
        sharedRuntime.reserveRuntime(queryId, sources);
        return sharedRuntime.getApplicationId();
      }
    }

    // If the query cannot be placed into any existing runtimes, we're forced to create a new one.
    final Set<SourceName> overlappingSourceTopics = new HashSet<>();
    for (final SourceName sourceTopic : sources) {
      for (final SharedKafkaStreamsRuntime sharedRuntime : sharedRuntimesByQueryLoad) {
        if (sharedRuntime.getSources().contains(sourceTopic)) {
          overlappingSourceTopics.add(sourceTopic);
          break;
        }
      }
    }
    log.info("Creating a new runtime as query {} cannot fit into any existing runtimes due to "
                 + "overlapping source topics: {}", queryId, overlappingSourceTopics);
    final SharedKafkaStreamsRuntime newRuntime =
        constructSharedKafkaStreamsRuntime(sharedRuntimesByAppId.size() + 1);
    newRuntime.reserveRuntime(queryId, sources);
    return newRuntime.getApplicationId();
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
    for (SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime : sharedRuntimesByAppId.values()) {
      sharedKafkaStreamsRuntime.close(closePersistent);
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

  // TODO: store changes in runtimes in command topic and maybe construct runtimes/id based on that
  private SharedKafkaStreamsRuntime constructSharedKafkaStreamsRuntime(final int runtimeIndex) {
    final String applicationId = QueryApplicationId.build(config, true, null) + runtimeIndex;

    final QueryErrorClassifier userErrorClassifiers = new MissingTopicClassifier(applicationId)
        .and(new AuthorizationClassifier(applicationId));
    final QueryErrorClassifier classifier = buildConfiguredClassifiers(config, applicationId)
        .map(userErrorClassifiers::and)
        .orElse(userErrorClassifiers);

    final SharedKafkaStreamsRuntime runtime =
        new SharedKafkaStreamsRuntimeImpl(
            kafkaStreamsBuilder,
            config.getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE),
            classifier,
            buildServerStreamsProperties(applicationId)
        );
    sharedRuntimesByAppId.put(runtime.getApplicationId(), runtime);
    return runtime;
  }

  private Map<String, Object> buildServerStreamsProperties(final String applicationId) {
    return buildStreamsProperties(
        config,
        applicationId,
        processingLogContext.getLoggerFactory().getLogger(applicationId));
  }

  public static Map<String, Object> buildStreamsProperties(
      final KsqlConfig config,
      final String applicationId,
      final ProcessingLogger logger
  ) {
    final Map<String, Object> newStreamsProperties =
        new HashMap<>(config.getKsqlStreamConfigProps(applicationId));

    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

    newStreamsProperties.put(
        ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER,
        logger
    );

    updateListProperty(
        newStreamsProperties,
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ConsumerCollector.class.getCanonicalName()
    );
    updateListProperty(
        newStreamsProperties,
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ProducerCollector.class.getCanonicalName()
    );
    updateListProperty(
        newStreamsProperties,
        StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
        RocksDBMetricsCollector.class.getName()
    );
    updateListProperty(
        newStreamsProperties,
        StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
        StorageUtilizationMetricsReporter.class.getName()
    );
    return newStreamsProperties;
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

  private static void updateListProperty(
      final Map<String, Object> properties,
      final String key,
      final Object value
  ) {
    final Object obj = properties.getOrDefault(key, new LinkedList<String>());
    final List<Object> valueList;
    // The property value is either a comma-separated string of class names, or a list of class
    // names
    if (obj instanceof String) {
      // If its a string just split it on the separator so we dont have to worry about adding a
      // separator
      final String asString = (String) obj;
      valueList = new LinkedList<>(Arrays.asList(asString.split("\\s*,\\s*")));
    } else if (obj instanceof List) {
      // The incoming list could be an instance of an immutable list. So we create a modifiable
      // List out of it to ensure that it is mutable.
      valueList = new LinkedList<>((List<?>) obj);
    } else {
      throw new KsqlException("Expecting list or string for property: " + key);
    }
    valueList.add(value);
    properties.put(key, valueList);
  }

  @FunctionalInterface
  interface QueryBuilderFactory {
    QueryBuilder create(
        SessionConfig config,
        ProcessingLogContext processingLogContext,
        ServiceContext serviceContext,
        FunctionRegistry functionRegistry,
        KafkaStreamsBuilder kafkaStreamsBuilder);
  }

  class ListenerImpl implements QueryMetadata.Listener {
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

      // If we had to add additional runtimes and just removed the last query from its runtime, we
      // can close it to reclaim the extra resources we had to put towards the new runtime;
      final SharedKafkaStreamsRuntime sharedRuntime =
          sharedRuntimesByAppId.get(queryMetadata.getQueryApplicationId());
      if (sharedRuntime != null
          && sharedRuntime.numberOfQueries() == 0
          && sharedRuntimesByAppId.size() > numInitialSharedRuntimes) {
        sharedRuntime.close(true);
      }
    }
  }
}
