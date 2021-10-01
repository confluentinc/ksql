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

import static io.confluent.ksql.query.QueryRegistryImpl.buildStreamsProperties;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
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
import io.confluent.ksql.execution.util.KeyUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
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
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryApplicationId;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.SharedRuntimePersistentQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.impl.ConcurrentHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;

/**
 * A builder for creating queries metadata.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class QueryBuilder {

  private static final String KSQL_THREAD_EXCEPTION_UNCAUGHT_LOGGER
      = "ksql.logger.thread.exception.uncaught";

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final SessionConfig config;
  private final ProcessingLogContext processingLogContext;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final MaterializationProviderBuilderFactory materializationProviderBuilderFactory;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;

  QueryBuilder(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final KafkaStreamsBuilder kafkaStreamsBuilder
  ) {
    this(
        config,
        processingLogContext,
        serviceContext,
        functionRegistry,
        kafkaStreamsBuilder,
        new MaterializationProviderBuilderFactory(
            config.getConfig(true),
            serviceContext,
            new KsMaterializationFactory(),
            new KsqlMaterializationFactory(processingLogContext)
        )
    );
  }

  @VisibleForTesting
  QueryBuilder(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final MaterializationProviderBuilderFactory materializationProviderBuilderFactory
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

    final Map<String, Object> streamsProperties =
        buildQueryStreamsProperties(applicationId, queryId);
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
      final Map<String, Object> streamsProperties,
      final String sourceApplicationId,
      final KsqlConfig ksqlConfig,
      final KsqlTopic ksqlTopic,
      final ServiceContext serviceContext
  ) {
    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED)) {
      return Optional.empty();
    }
    final boolean isTable;
    if (result instanceof KTableHolder) {
      isTable = true;
    } else {
      isTable = false;
    }
    final Optional<ScalablePushRegistry> registry = ScalablePushRegistry.create(schema,
        allPersistentQueries, isTable, streamsProperties,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY),
        ksqlConfig.originals(), sourceApplicationId,
        ksqlTopic, serviceContext, ksqlConfig);
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
    final Map<String, Object> streamsProperties =
        buildQueryStreamsProperties(applicationId, queryId);

    final LogicalSchema logicalSchema;
    final KeyFormat keyFormat;
    final ValueFormat valueFormat;
    final KsqlTopic ksqlTopic;

    switch (persistentQueryType) {
      // CREATE_SOURCE does not have a sink, so the schema is obtained from the query source
      case CREATE_SOURCE:
        final DataSource dataSource = Iterables.getOnlyElement(sources);

        logicalSchema = dataSource.getSchema();
        keyFormat = dataSource.getKsqlTopic().getKeyFormat();
        valueFormat = dataSource.getKsqlTopic().getValueFormat();
        ksqlTopic = dataSource.getKsqlTopic();

        break;
      default:
        logicalSchema = sinkDataSource.get().getSchema();
        keyFormat = sinkDataSource.get().getKsqlTopic().getKeyFormat();
        valueFormat = sinkDataSource.get().getKsqlTopic().getValueFormat();
        ksqlTopic = sinkDataSource.get().getKsqlTopic();

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

    final Optional<ScalablePushRegistry> scalablePushRegistry = applyScalablePushProcessor(
        querySchema.logicalSchema(),
        result,
        allPersistentQueries,
        streamsProperties,
        applicationId,
        ksqlConfig,
        ksqlTopic,
        serviceContext
    );

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

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  SharedRuntimePersistentQueryMetadata buildPersistentQueryInSharedRuntime(
      final KsqlConfig ksqlConfig,
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementText,
      final QueryId queryId,
      final Optional<DataSource> sinkDataSource,
      final Set<DataSource> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final QueryMetadata.Listener listener,
      final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries
  ) {
    final String applicationId = sharedKafkaStreamsRuntime.getApplicationId();
    final Map<String, Object> queryOverrides =
        buildQueryStreamsProperties(applicationId, queryId);

    final LogicalSchema logicalSchema;
    final KeyFormat keyFormat;
    final ValueFormat valueFormat;
    final KsqlTopic ksqlTopic;

    switch (persistentQueryType) {
      // CREATE_SOURCE does not have a sink, so the schema is obtained from the query source
      case CREATE_SOURCE:
        final DataSource dataSource = Iterables.getOnlyElement(sources);

        logicalSchema = dataSource.getSchema();
        keyFormat = dataSource.getKsqlTopic().getKeyFormat();
        valueFormat = dataSource.getKsqlTopic().getValueFormat();
        ksqlTopic = dataSource.getKsqlTopic();

        break;
      default:
        logicalSchema = sinkDataSource.get().getSchema();
        keyFormat = sinkDataSource.get().getKsqlTopic().getKeyFormat();
        valueFormat = sinkDataSource.get().getKsqlTopic().getValueFormat();
        ksqlTopic = sinkDataSource.get().getKsqlTopic();

        break;
    }

    final PhysicalSchema querySchema = PhysicalSchema.from(
        logicalSchema,
        keyFormat.getFeatures(),
        valueFormat.getFeatures()
    );

    final NamedTopologyBuilder namedTopologyBuilder = sharedKafkaStreamsRuntime
        .getKafkaStreams()
        .newNamedTopologyBuilder(queryId.toString(), PropertiesUtil.asProperties(queryOverrides));

    final RuntimeBuildContext runtimeBuildContext = buildContext(
            applicationId,
            queryId,
            namedTopologyBuilder
    );
    final Object result = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    final NamedTopology topology = namedTopologyBuilder.build();

    final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
            materializationProviderBuilder = getMaterializationInfo(result).map(info ->
            materializationProviderBuilderFactory.materializationProviderBuilder(
                    info,
                    querySchema,
                    keyFormat,
                    queryOverrides,
                    applicationId
            ));

    final QueryErrorClassifier userErrorClassifiers =
        new MissingTopicClassifier(applicationId)
            .and(new AuthorizationClassifier(applicationId));
    final QueryErrorClassifier classifier = buildConfiguredClassifiers(ksqlConfig, applicationId)
            .map(userErrorClassifiers::and)
            .orElse(userErrorClassifiers);

    final Optional<ScalablePushRegistry> scalablePushRegistry = applyScalablePushProcessor(
        querySchema.logicalSchema(),
        result,
        allPersistentQueries,
        queryOverrides,
        applicationId,
        ksqlConfig,
        ksqlTopic,
        serviceContext
    );

    return new SharedRuntimePersistentQueryMetadata(
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
        (builder) -> buildContext(applicationId, queryId, builder),
        getUncaughtExceptionProcessingLogger(queryId),
        sinkDataSource,
        listener,
        classifier,
        queryOverrides,
        scalablePushRegistry,
        config.getConfig(true).getInt(KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE)
    );
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
      kstream.process(TransientQuerySinkProcessor.supplier(queue, endOffsets, donePartitions));

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

  public static Object buildQueryImplementation(
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

  private Map<String, Object> buildQueryStreamsProperties(
      final String applicationId,
      final QueryId queryId
  ) {
    return buildStreamsProperties(
        config.getConfig(true),
        applicationId,
        processingLogContext.getLoggerFactory().getLogger(queryId.toString())
    );
  }

  static Optional<QueryErrorClassifier> buildConfiguredClassifiers(
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
}
