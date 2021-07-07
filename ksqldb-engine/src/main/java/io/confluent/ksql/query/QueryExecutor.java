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
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryApplicationId;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class QueryExecutor {

  private static final String KSQL_THREAD_EXCEPTION_UNCAUGHT_LOGGER
      = "ksql.logger.thread.exception.uncaught";

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final SessionConfig config;
  private final ProcessingLogContext processingLogContext;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final StreamsBuilder streamsBuilder;
  private final MaterializationProviderBuilderFactory materializationProviderBuilderFactory;

  QueryExecutor(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry) {
    this(
        config,
        processingLogContext,
        serviceContext,
        functionRegistry,
        new KafkaStreamsBuilderImpl(
            Objects.requireNonNull(serviceContext, "serviceContext").getKafkaClientSupplier()),
        new StreamsBuilder(),
        new MaterializationProviderBuilderFactory(
            config.getConfig(true),
            serviceContext,
            new KsMaterializationFactory(),
            new KsqlMaterializationFactory(processingLogContext)
        )
    );
  }

  @VisibleForTesting
  QueryExecutor(
      final SessionConfig config,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final StreamsBuilder streamsBuilder,
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
    this.streamsBuilder = Objects.requireNonNull(streamsBuilder, "streamsBuilder");
    this.materializationProviderBuilderFactory = Objects.requireNonNull(
        materializationProviderBuilderFactory,
        "materializationProviderBuilderFactory"
    );
  }

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
      final QueryMetadata.Listener listener
  ) {
    final KsqlConfig ksqlConfig = config.getConfig(true);
    final String applicationId = QueryApplicationId.build(ksqlConfig, false, queryId);
    final RuntimeBuildContext runtimeBuildContext = buildContext(applicationId, queryId);

    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);
    final Object buildResult = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    final BlockingRowQueue queue = buildTransientQueryQueue(buildResult, limit, excludeTombstones);
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
    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_ENABLED)) {
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
        allPersistentQueries, isTable, windowed, streamsProperties);
    registry.ifPresent(r -> stream.process(registry.get()));
    return registry;
  }

  PersistentQueryMetadata buildPersistentQuery(
      final KsqlConstants.PersistentQueryType persistentQueryType,
      final String statementText,
      final QueryId queryId,
      final DataSource sinkDataSource,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final QueryMetadata.Listener listener,
      final Supplier<List<PersistentQueryMetadata>> allPersistentQueries
  ) {
    final KsqlConfig ksqlConfig = config.getConfig(true);

    final String applicationId = QueryApplicationId.build(ksqlConfig, true, queryId);
    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);

    final PhysicalSchema querySchema = PhysicalSchema.from(
        sinkDataSource.getSchema(),
        sinkDataSource.getKsqlTopic().getKeyFormat().getFeatures(),
        sinkDataSource.getKsqlTopic().getValueFormat().getFeatures()
    );

    final RuntimeBuildContext runtimeBuildContext = buildContext(applicationId, queryId);
    final Object result = buildQueryImplementation(physicalPlan, runtimeBuildContext);
    // Creates a ProcessorSupplier, a ScalablePushRegistry, to apply to the topology, if
    // scalable push queries are enabled.
    final Optional<ScalablePushRegistry> scalablePushRegistry
        = applyScalablePushProcessor(querySchema.logicalSchema(), result, allPersistentQueries,
        sinkDataSource.getKsqlTopic().getKeyFormat().isWindowed(),
        streamsProperties, ksqlConfig);
    final Topology topology = streamsBuilder.build(PropertiesUtil.asProperties(streamsProperties));

    final Optional<MaterializationProviderBuilderFactory.MaterializationProviderBuilder>
        materializationProviderBuilder = getMaterializationInfo(result).map(info ->
            materializationProviderBuilderFactory.materializationProviderBuilder(
                info,
                querySchema,
                sinkDataSource.getKsqlTopic().getKeyFormat(),
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
        sources,
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

  private ProcessingLogger getUncaughtExceptionProcessingLogger(final QueryId queryId) {
    final QueryContext.Stacker stacker = new QueryContext.Stacker()
        .push(KSQL_THREAD_EXCEPTION_UNCAUGHT_LOGGER);

    return processingLogContext.getLoggerFactory().getLogger(
        QueryLoggerUtil.queryLoggerName(queryId, stacker.getQueryContext()));
  }

  private static TransientQueryQueue buildTransientQueryQueue(
      final Object buildResult,
      final OptionalInt limit,
      final boolean excludeTombstones
  ) {
    final TransientQueryQueue queue = new TransientQueryQueue(limit);

    if (buildResult instanceof KStreamHolder<?>) {
      final KStream<?, GenericRow> kstream = ((KStreamHolder<?>) buildResult).getStream();

      kstream
          // Null value for a stream is invalid:
          .filter((k, v) -> v != null)
          .foreach((k, v) -> queue.acceptRow(null, v));

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

  private RuntimeBuildContext buildContext(final String applicationId, final QueryId queryId) {
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
}
