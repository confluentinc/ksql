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

package io.confluent.ksql.query;

import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.Builder;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory;
import io.confluent.ksql.execution.streams.metrics.RocksDBMetricsCollector;
import io.confluent.ksql.execution.util.KeyUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.StorageUtilizationMetricsReporter;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueriesInSharedRuntimesImpl;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryApplicationId;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.SharedKafkaStreamsRuntimeImpl;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.ValidationSharedKafkaStreamsRuntimeImpl;
import io.vertx.core.impl.ConcurrentHashSet;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;

/**
 * A builder for creating queries metadata.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class QueryBuilder {

  private static final String KSQL_THREAD_EXCEPTION_UNCAUGHT_LOGGER
      = "ksql.logger.thread.exception.uncaught";

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final SessionConfig config;
  private final ProcessingLogContext processingLogContext;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final MaterializationProviderBuilderFactory materializationProviderBuilderFactory;
  private final List<SharedKafkaStreamsRuntime> streams;
  private final boolean real;

  QueryBuilder(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final List<SharedKafkaStreamsRuntime> streams,
      final boolean real
  ) {
    this(
        config,
        processingLogContext,
        serviceContext,
        functionRegistry,
        new KafkaStreamsBuilderImpl(
            Objects.requireNonNull(serviceContext, "serviceContext").getKafkaClientSupplier()),
        new MaterializationProviderBuilderFactory(
            config.getConfig(true),
            serviceContext,
            new KsMaterializationFactory(),
            new KsqlMaterializationFactory(processingLogContext)
        ),
        streams,
        real
    );
  }

  @VisibleForTesting
  QueryBuilder(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final MaterializationProviderBuilderFactory materializationProviderBuilderFactory,
      final List<SharedKafkaStreamsRuntime> streams,
      final boolean real
  ) {
    this.config = Objects.requireNonNull(config, "config");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder, "kafkaStreamsBuilder");
    this.materializationProviderBuilderFactory = Objects.requireNonNull(
        materializationProviderBuilderFactory,
        "materializationProviderBuilderFactory"
    );
    this.streams = Objects.requireNonNull(streams, "streams");
    this.real = real;
  }

  @SuppressWarnings("ParameterNumber")
  TransientQueryMetadata buildTransientQuery(
      final String statementText,
      final QueryId queryId,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<WindowInfo> windowInfo,
      final boolean excludeTombstones,
      final QueryMetadata.Listener listener,
      final StreamsBuilder streamsBuilder,
      final Optional<ImmutableMap<TopicPartition, Long>> endOffsets
  ) {
    final KsqlConfig ksqlConfig = config.getConfig(true);
    final String applicationId = QueryApplicationId.build(ksqlConfig, false, queryId);
    final RuntimeBuildContext runtimeBuildContext = buildContext(
        applicationId,
        queryId,
        streamsBuilder
    );

    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);
    final Object buildResult = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    final TransientQueryQueue queue =
        buildTransientQueryQueue(buildResult, limit, excludeTombstones, endOffsets);
    final Topology topology = streamsBuilder.build(PropertiesUtil.asProperties(streamsProperties));

    final TransientQueryMetadata.ResultType resultType = buildResult instanceof KTableHolder
        ? windowInfo.isPresent() ? ResultType.WINDOWED_TABLE : ResultType.TABLE
        : ResultType.STREAM;

    return new TransientQueryMetadata(
        statementText,
        schema,
        sources,
        planSummary,
        queue,
        queryId,
        applicationId,
        topology,
        kafkaStreamsBuilder,
        streamsProperties,
        config.getOverrides(),
        ksqlConfig.getLong(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG),
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE),
        resultType,
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS),
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS),
        listener
    );
  }

  private static Optional<MaterializationInfo> getMaterializationInfo(final Object result) {
    if (result instanceof KTableHolder) {
      return ((KTableHolder<?>) result).getMaterializationBuilder().map(Builder::build);
    }
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static Optional<ScalablePushRegistry> applyScalablePushProcessor(
      final LogicalSchema schema,
      final Object result,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final boolean windowed,
      final Map<String, Object> streamsProperties,
      final KsqlConfig ksqlConfig
  ) {
    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED)) {
      return Optional.empty();
    }
    final KStream<?, GenericRow> stream;
    final boolean isTable;
    if (result instanceof KTableHolder) {
      stream = ((KTableHolder<?>) result).getTable().toStream();
      isTable = true;
    } else {
      stream = ((KStreamHolder<?>) result).getStream();
      isTable = false;
    }
    final Optional<ScalablePushRegistry> registry = ScalablePushRegistry.create(schema,
        allPersistentQueries, isTable, windowed, streamsProperties,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY));
    registry.ifPresent(r -> stream.process(registry.get()));
    return registry;
  }

  @SuppressWarnings("ParameterNumber")
  PersistentQueryMetadata buildPersistentQueryInDedicatedRuntime(
      final KsqlConfig ksqlConfig,
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementText,
      final QueryId queryId,
      final Optional<DataSource> sinkDataSource,
      final Set<DataSource> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final QueryMetadata.Listener listener,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries,
      final StreamsBuilder streamsBuilder) {

    final String applicationId = QueryApplicationId.build(ksqlConfig, true, queryId);
    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);

    final LogicalSchema logicalSchema;
    final KeyFormat keyFormat;
    final ValueFormat valueFormat;

    switch (persistentQueryType) {
      // CREATE_SOURCE does not have a sink, so the schema is obtained from the query source
      case CREATE_SOURCE:
        final DataSource dataSource = Iterables.getOnlyElement(sources);

        logicalSchema = dataSource.getSchema();
        keyFormat = dataSource.getKsqlTopic().getKeyFormat();
        valueFormat = dataSource.getKsqlTopic().getValueFormat();

        break;
      default:
        logicalSchema = sinkDataSource.get().getSchema();
        keyFormat = sinkDataSource.get().getKsqlTopic().getKeyFormat();
        valueFormat = sinkDataSource.get().getKsqlTopic().getValueFormat();

        break;
    }

    final PhysicalSchema querySchema = PhysicalSchema.from(
        logicalSchema,
        keyFormat.getFeatures(),
        valueFormat.getFeatures()
    );

    final RuntimeBuildContext runtimeBuildContext = buildContext(
        applicationId,
        queryId,
        streamsBuilder
    );
    final Object result = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    // Creates a ProcessorSupplier, a ScalablePushRegistry, to apply to the topology, if
    // scalable push queries are enabled.
    final Optional<ScalablePushRegistry> scalablePushRegistry
        = applyScalablePushProcessor(
            querySchema.logicalSchema(),
            result,
            allPersistentQueries,
            keyFormat.isWindowed(),
            streamsProperties,
            ksqlConfig
    );
    final Topology topology = streamsBuilder
            .build(PropertiesUtil.asProperties(streamsProperties));

    final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
        materializationProviderBuilder = getMaterializationInfo(result).map(info ->
            materializationProviderBuilderFactory.materializationProviderBuilder(
                info,
                querySchema,
                keyFormat,
                streamsProperties,
                applicationId
            ));

    final QueryErrorClassifier userErrorClassifiers = new MissingTopicClassifier(applicationId)
        .and(new AuthorizationClassifier(applicationId));
    final QueryErrorClassifier classifier = buildConfiguredClassifiers(ksqlConfig, applicationId)
        .map(userErrorClassifiers::and)
        .orElse(userErrorClassifiers);

    return new PersistentQueryMetadataImpl(
        persistentQueryType,
        statementText,
        querySchema,
        sources.stream().map(DataSource::getName).collect(Collectors.toSet()),
        sinkDataSource,
        planSummary,
        queryId,
        materializationProviderBuilder,
        applicationId,
        topology,
        kafkaStreamsBuilder,
        runtimeBuildContext.getSchemas(),
        streamsProperties,
        config.getOverrides(),
        ksqlConfig.getLong(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG),
        classifier,
        physicalPlan,
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE),
        getUncaughtExceptionProcessingLogger(queryId),
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS),
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS),
        listener,
        scalablePushRegistry
    );

  }

  PersistentQueryMetadata buildPersistentQueryInSharedRuntime(
      final KsqlConfig ksqlConfig,
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementText,
      final QueryId queryId,
      final Optional<DataSource> sinkDataSource,
      final Set<DataSource> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final QueryMetadata.Listener listener,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries
  ) {
    final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime = getKafkaStreamsInstance(
        sources.stream().map(DataSource::getName).collect(Collectors.toSet()),
        queryId);

    final String applicationId = sharedKafkaStreamsRuntime
            .getStreamProperties()
            .get(StreamsConfig.APPLICATION_ID_CONFIG)
            .toString();
    final Map<String, Object> streamsProperties = sharedKafkaStreamsRuntime.getStreamProperties();

    final LogicalSchema logicalSchema;
    final KeyFormat keyFormat;
    final ValueFormat valueFormat;

    switch (persistentQueryType) {
      // CREATE_SOURCE does not have a sink, so the schema is obtained from the query source
      case CREATE_SOURCE:
        final DataSource dataSource = Iterables.getOnlyElement(sources);

        logicalSchema = dataSource.getSchema();
        keyFormat = dataSource.getKsqlTopic().getKeyFormat();
        valueFormat = dataSource.getKsqlTopic().getValueFormat();

        break;
      default:
        logicalSchema = sinkDataSource.get().getSchema();
        keyFormat = sinkDataSource.get().getKsqlTopic().getKeyFormat();
        valueFormat = sinkDataSource.get().getKsqlTopic().getValueFormat();

        break;
    }

    final PhysicalSchema querySchema = PhysicalSchema.from(
        logicalSchema,
        keyFormat.getFeatures(),
        valueFormat.getFeatures()
    );
    final NamedTopologyStreamsBuilder namedTopologyStreamsBuilder = new NamedTopologyStreamsBuilder(
            queryId.toString()
    );

    final RuntimeBuildContext runtimeBuildContext = buildContext(
            applicationId,
            queryId,
            namedTopologyStreamsBuilder
    );
    final Object result = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    // Creates a ProcessorSupplier, a ScalablePushRegistry, to apply to the topology, if
    // scalable push queries are enabled.
    final Optional<ScalablePushRegistry> scalablePushRegistry
            = applyScalablePushProcessor(
            querySchema.logicalSchema(),
            result,
            allPersistentQueries,
            keyFormat.isWindowed(),
            streamsProperties,
            ksqlConfig
    );
    final NamedTopology topology = namedTopologyStreamsBuilder
            .buildNamedTopology(PropertiesUtil.asProperties(streamsProperties));

    final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
            materializationProviderBuilder = getMaterializationInfo(result).map(info ->
            materializationProviderBuilderFactory.materializationProviderBuilder(
                    info,
                    querySchema,
                    keyFormat,
                    streamsProperties,
                    applicationId
            ));

    final QueryErrorClassifier userErrorClassifiers = new MissingTopicClassifier(applicationId)
            .and(new AuthorizationClassifier(applicationId));
    final QueryErrorClassifier classifier = buildConfiguredClassifiers(ksqlConfig, applicationId)
            .map(userErrorClassifiers::and)
            .orElse(userErrorClassifiers);

    return new PersistentQueriesInSharedRuntimesImpl(
        persistentQueryType,
        statementText,
        querySchema,
        sources.stream().map(DataSource::getName).collect(Collectors.toSet()),
        planSummary,
        applicationId,
        topology,
        sharedKafkaStreamsRuntime,
        runtimeBuildContext.getSchemas(),
        config.getOverrides(),
        queryId,
        materializationProviderBuilder,
        physicalPlan,
        getUncaughtExceptionProcessingLogger(queryId),
        sinkDataSource,
        listener,
        classifier,
        streamsProperties,
        scalablePushRegistry
    );
  }

  private SharedKafkaStreamsRuntime getKafkaStreamsInstance(
          final Set<SourceName> sources,
          final QueryId queryID) {
    for (final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime : streams) {
      if (sharedKafkaStreamsRuntime.getSources().stream().noneMatch(sources::contains)
          || sharedKafkaStreamsRuntime.getQueries().contains(queryID)) {
        sharedKafkaStreamsRuntime.markSources(queryID, sources);
        return sharedKafkaStreamsRuntime;
      }
    }
    final SharedKafkaStreamsRuntime stream;
    if (real) {
      stream = new SharedKafkaStreamsRuntimeImpl(
          kafkaStreamsBuilder,
          config.getConfig(true).getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE),
          buildStreamsProperties(
              "_confluent-ksql-" + streams.size() + "-" + UUID.randomUUID(),
              queryID)
      );
    } else {
      stream = new ValidationSharedKafkaStreamsRuntimeImpl(
          kafkaStreamsBuilder,
          config.getConfig(true).getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE),
          buildStreamsProperties(
              "_confluent-ksql-" + streams.size() + "-" + UUID.randomUUID() + "-validation",
              queryID)
      );
    }
    streams.add(stream);
    stream.markSources(queryID, sources);
    return stream;

  }

  private ProcessingLogger getUncaughtExceptionProcessingLogger(final QueryId queryId) {
    final QueryContext.Stacker stacker = new QueryContext.Stacker()
        .push(KSQL_THREAD_EXCEPTION_UNCAUGHT_LOGGER);

    return processingLogContext.getLoggerFactory().getLogger(
        QueryLoggerUtil.queryLoggerName(queryId, stacker.getQueryContext()));
  }

  private static TransientQueryQueue buildTransientQueryQueue(
      final Object buildResult,
      final OptionalInt limit,
      final boolean excludeTombstones,
      final Optional<ImmutableMap<TopicPartition, Long>> endOffsets
  ) {
    final TransientQueryQueue queue = new TransientQueryQueue(limit);

    if (buildResult instanceof KStreamHolder<?>) {
      final KStream<?, GenericRow> kstream = ((KStreamHolder<?>) buildResult).getStream();
      // The list of "done partitions" is shared among all the stream threads and tasks.
      final ConcurrentHashSet<TopicPartition> donePartitions = new ConcurrentHashSet<>();
      kstream.process(
          (ProcessorSupplier<Object, GenericRow, Void, Void>) () ->
              new TransientQuerySinkProcessor(queue, endOffsets, donePartitions)
      );

    } else if (buildResult instanceof KTableHolder<?>) {
      final KTable<?, GenericRow> ktable = ((KTableHolder<?>) buildResult).getTable();
      final KStream<?, GenericRow> stream = ktable.toStream();

      final KStream<?, GenericRow> filtered = excludeTombstones
          ? stream.filter((k, v) -> v != null)
          : stream;

      filtered.foreach((k, v) -> queue.acceptRow(KeyUtil.asList(k), v));
    } else {
      throw new IllegalStateException("Unexpected type built from execution plan");
    }

    return queue;
  }

  private static Object buildQueryImplementation(
      final ExecutionStep<?> physicalPlan,
      final RuntimeBuildContext runtimeBuildContext
  ) {
    final PlanBuilder planBuilder = new KSPlanBuilder(runtimeBuildContext);
    return physicalPlan.build(planBuilder);
  }

  private RuntimeBuildContext buildContext(
          final String applicationId,
          final QueryId queryId,
          final StreamsBuilder streamsBuilder) {
    return RuntimeBuildContext.of(
        streamsBuilder,
        config.getConfig(true),
        serviceContext,
        processingLogContext,
        functionRegistry,
        applicationId,
        queryId
    );
  }

  private Map<String, Object> buildStreamsProperties(
      final String applicationId,
      final QueryId queryId
  ) {
    final Map<String, Object> newStreamsProperties
        = new HashMap<>(config.getConfig(true).getKsqlStreamConfigProps(applicationId));
    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    final ProcessingLogger logger
        = processingLogContext.getLoggerFactory().getLogger(queryId.toString());
    newStreamsProperties.put(
        ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER,
        logger);

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

  private static Optional<QueryErrorClassifier> buildConfiguredClassifiers(
      final KsqlConfig cfg,
      final String queryId
  ) {
    final Map<String, Object> regexPrefixes = cfg.originalsWithPrefix(
        KsqlConfig.KSQL_ERROR_CLASSIFIER_REGEX_PREFIX
    );

    final ImmutableList.Builder<QueryErrorClassifier> builder = ImmutableList.builder();
    for (final Object value : regexPrefixes.values()) {
      final String classifier = (String) value;
      builder.add(RegexClassifier.fromConfig(classifier, queryId));
    }
    final ImmutableList<QueryErrorClassifier> classifiers = builder.build();

    if (classifiers.isEmpty()) {
      return Optional.empty();
    }

    QueryErrorClassifier combined = Iterables.get(classifiers, 0);
    for (final QueryErrorClassifier classifier : Iterables.skip(classifiers, 1)) {
      combined = combined.and(classifier);
    }
    return Optional.ofNullable(combined);
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

  private static class TransientQuerySinkProcessor implements
      Processor<Object, GenericRow, Void, Void> {

    private final TransientQueryQueue queue;
    private final Optional<ImmutableMap<TopicPartition, Long>> endOffsets;
    private final ConcurrentHashSet<TopicPartition> donePartitions;
    private ProcessorContext<Void, Void> context;

    TransientQuerySinkProcessor(
        final TransientQueryQueue queue,
        final Optional<ImmutableMap<TopicPartition, Long>> endOffsets,
        final ConcurrentHashSet<TopicPartition> donePartitions) {

      this.queue = queue;
      this.endOffsets = endOffsets;
      this.donePartitions = donePartitions;
    }

    @Override
    public void init(final ProcessorContext<Void, Void> context) {
      Processor.super.init(context);
      this.context = context;
      if (endOffsets.isPresent()) {
        // registering a punctuation here is the easiest way to check
        // if we're complete even if we don't see all records.
        // The in-line check in process() is faster,
        // but some records may be filtered out before reaching us.
        context.schedule(
            Duration.ofMillis(100),
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> checkCompletion()
        );
      }
    }

    @Override
    public void process(final Record<Object, GenericRow> record) {
      final Optional<TopicPartition> topicPartition =
          context
              .recordMetadata()
              .map(m -> new TopicPartition(m.topic(), m.partition()));

      final boolean alreadyDone =
          topicPartition.isPresent() && donePartitions.contains(topicPartition.get());

      // We ignore null values in streams as invalid data and drop the record.
      // If we're already done with the current partition, we just drop the record.
      if (record.value() != null && !alreadyDone) {
        queue.acceptRow(null, record.value());
      }

      if (topicPartition.isPresent()) {
        checkCompletion(topicPartition.get());
      }
    }

    private void checkCompletion(final TopicPartition topicPartition) {
      if (endOffsets.isPresent()) {
        final ImmutableMap<TopicPartition, Long> endOffsetsMap = endOffsets.get();
        if (endOffsetsMap.containsKey(topicPartition)) {
          final Map<TopicPartition, OffsetAndMetadata> currentPositions = getCurrentPositions();
          checkCompletion(topicPartition, currentPositions, endOffsetsMap.get(topicPartition));
        }
      }
    }

    private void checkCompletion() {
      if (endOffsets.isPresent()) {
        final Map<TopicPartition, OffsetAndMetadata> currentPositions =
            getCurrentPositions();

        for (final Entry<TopicPartition, Long> end : endOffsets.get().entrySet()) {
          checkCompletion(
              end.getKey(),
              currentPositions,
              end.getValue()
          );
        }
        // if we're completely done with this query, then call the completion handler.
        if (ImmutableSet.copyOf(donePartitions).equals(endOffsets.get().keySet())) {
          queue.complete();
        }
      }
    }

    private void checkCompletion(final TopicPartition topicPartition,
        final Map<TopicPartition, OffsetAndMetadata> currentPositions, final Long end) {
      if (currentPositions.containsKey(topicPartition)) {
        final OffsetAndMetadata current = currentPositions.get(topicPartition);
        if (current != null && current.offset() >= end) {
          donePartitions.add(topicPartition);
        }
      }
    }

    @SuppressWarnings("unchecked")
    private Map<TopicPartition, OffsetAndMetadata> getCurrentPositions() {
      // It might make sense to actually propose adding the concept of getting the
      // "current position" in the ProcessorContext, but for now we just expose it
      // by reflection. Obviously, this code is pretty brittle.
      try {
        if (context.getClass().equals(ProcessorContextImpl.class)) {
          final Field streamTask;
          streamTask = ProcessorContextImpl.class.getDeclaredField("streamTask");
          streamTask.setAccessible(true);
          final StreamTask task = (StreamTask) streamTask.get(context);
          final Method committableOffsetsAndMetadata =
              StreamTask.class.getDeclaredMethod("committableOffsetsAndMetadata");
          committableOffsetsAndMetadata.setAccessible(true);
          return (Map<TopicPartition, OffsetAndMetadata>)
              committableOffsetsAndMetadata.invoke(task);
        } else {
          // Specifically, this will break if you try to run this processor in a
          // unit test with a mock processor. Both KafkaStreams and the TopologyTestDriver
          // use a ProcessorContextImpl, which should be the only way this processor gets run.
          throw new IllegalStateException(
              "Expected only to run in the KafkaStreams or TopologyTestDriver runtimes."
          );
        }
      } catch (final NoSuchFieldException | IllegalAccessException
          | NoSuchMethodException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

  }
}
