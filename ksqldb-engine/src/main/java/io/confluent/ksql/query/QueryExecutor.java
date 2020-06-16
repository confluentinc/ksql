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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.Builder;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterialization;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializationFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder.BuildResult;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Sink;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class QueryExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final KsqlConfig ksqlConfig;
  private final Map<String, Object> overrides;
  private final ProcessingLogContext processingLogContext;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Consumer<QueryMetadata> queryCloseCallback;
  private final KsMaterializationFactory ksMaterializationFactory;
  private final KsqlMaterializationFactory ksqlMaterializationFactory;
  private final StreamsBuilder streamsBuilder;

  public QueryExecutor(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final Consumer<QueryMetadata> queryCloseCallback) {
    this(
        ksqlConfig,
        overrides,
        processingLogContext,
        serviceContext,
        functionRegistry,
        queryCloseCallback,
        new KafkaStreamsBuilderImpl(
            Objects.requireNonNull(serviceContext, "serviceContext").getKafkaClientSupplier()),
        new StreamsBuilder(),
        new KsqlMaterializationFactory(processingLogContext),
        new KsMaterializationFactory()
    );
  }

  QueryExecutor(
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overrides,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final Consumer<QueryMetadata> queryCloseCallback,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final StreamsBuilder streamsBuilder,
      final KsqlMaterializationFactory ksqlMaterializationFactory,
      final KsMaterializationFactory ksMaterializationFactory) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.overrides = Objects.requireNonNull(overrides, "overrides");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.queryCloseCallback = Objects.requireNonNull(
        queryCloseCallback,
        "queryCloseCallback"
    );
    this.ksMaterializationFactory = Objects.requireNonNull(
        ksMaterializationFactory,
        "ksMaterializationFactory"
    );
    this.ksqlMaterializationFactory = Objects.requireNonNull(
        ksqlMaterializationFactory,
        "ksqlMaterializationFactory"
    );
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder);
    this.streamsBuilder = Objects.requireNonNull(streamsBuilder, "builder");
  }

  public TransientQueryMetadata buildTransientQuery(
      final String statementText,
      final QueryId queryId,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final LogicalSchema schema,
      final OptionalInt limit
  ) {
    final BlockingRowQueue queue = buildTransientQueryQueue(queryId, physicalPlan, limit);

    final String applicationId = addTimeSuffix(getQueryApplicationId(
        getServiceId(),
        ksqlConfig.getString(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG),
        queryId
    ));

    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);

    final BuildResult built =
        kafkaStreamsBuilder.buildKafkaStreams(streamsBuilder, streamsProperties);

    return new TransientQueryMetadata(
        statementText,
        built.kafkaStreams,
        schema,
        sources,
        planSummary,
        queue,
        applicationId,
        built.topology,
        streamsProperties,
        overrides,
        queryCloseCallback,
        ksqlConfig.getLong(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG)
    );
  }

  private static Optional<MaterializationInfo> getMaterializationInfo(final Object result) {
    if (result instanceof KTableHolder) {
      return ((KTableHolder<?>) result).getMaterializationBuilder().map(Builder::build);
    }
    return Optional.empty();
  }

  public PersistentQueryMetadata buildQuery(
      final String statementText,
      final QueryId queryId,
      final DataSource sinkDataSource,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary
  ) {
    final KsqlQueryBuilder ksqlQueryBuilder = queryBuilder(queryId);
    final PlanBuilder planBuilder = new KSPlanBuilder(ksqlQueryBuilder);
    final Object result = physicalPlan.build(planBuilder);
    final String persistenceQueryPrefix =
        ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);
    final String applicationId = getQueryApplicationId(
        getServiceId(),
        persistenceQueryPrefix,
        queryId
    );
    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);
    final BuildResult built =
        kafkaStreamsBuilder.buildKafkaStreams(streamsBuilder, streamsProperties);

    final PhysicalSchema querySchema = PhysicalSchema.from(
        sinkDataSource.getSchema(),
        sinkDataSource.getSerdeOptions()
    );
    final Optional<MaterializationProvider> materializationBuilder = getMaterializationInfo(result)
        .flatMap(info -> buildMaterializationProvider(
            info,
            built.kafkaStreams,
            querySchema,
            sinkDataSource.getKsqlTopic().getKeyFormat(),
            streamsProperties,
            applicationId
        ));

    final QueryErrorClassifier topicClassifier = new MissingTopicClassifier(
        applicationId,
        extractTopics(built.topology),
        serviceContext.getTopicClient());
    final QueryErrorClassifier classifier = buildConfiguredClassifiers(ksqlConfig, applicationId)
        .map(topicClassifier::and)
        .orElse(topicClassifier);

    return new PersistentQueryMetadata(
        statementText,
        built.kafkaStreams,
        querySchema,
        sources,
        sinkDataSource.getName(),
        planSummary,
        queryId,
        sinkDataSource.getDataSourceType(),
        materializationBuilder,
        applicationId,
        sinkDataSource.getKsqlTopic(),
        built.topology,
        ksqlQueryBuilder.getSchemas(),
        streamsProperties,
        overrides,
        queryCloseCallback,
        ksqlConfig.getLong(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG),
        classifier);
  }

  private TransientQueryQueue buildTransientQueryQueue(
      final QueryId queryId,
      final ExecutionStep<?> physicalPlan,
      final OptionalInt limit) {
    final KsqlQueryBuilder ksqlQueryBuilder = queryBuilder(queryId);
    final PlanBuilder planBuilder = new KSPlanBuilder(ksqlQueryBuilder);
    final Object buildResult = physicalPlan.build(planBuilder);
    final KStream<?, GenericRow> kstream;
    if (buildResult instanceof KStreamHolder<?>) {
      kstream = ((KStreamHolder<?>) buildResult).getStream();
    } else if (buildResult instanceof KTableHolder<?>) {
      final KTable<?, GenericRow> ktable = ((KTableHolder<?>) buildResult).getTable();
      kstream = ktable.toStream();
    } else {
      throw new IllegalStateException("Unexpected type built from exection plan");
    }
    final TransientQueryQueue queue = new TransientQueryQueue(limit);
    kstream.foreach((k, v) -> queue.acceptRow(v));
    return queue;
  }

  private KsqlQueryBuilder queryBuilder(final QueryId queryId) {
    return KsqlQueryBuilder.of(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );
  }

  private String getServiceId() {
    return ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }

  private Map<String, Object> buildStreamsProperties(
      final String applicationId,
      final QueryId queryId
  ) {
    final Map<String, Object> newStreamsProperties
        = new HashMap<>(ksqlConfig.getKsqlStreamConfigProps(applicationId));
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

  private static Set<String> extractTopics(final Topology topology) {
    final Set<String> usedTopics = new HashSet<>();
    for (final Subtopology subtopology : topology.describe().subtopologies()) {
      for (final Node node : subtopology.nodes()) {
        if (node instanceof Source) {
          usedTopics.addAll(((Source) node).topicSet());
        } else if (node instanceof Sink) {
          usedTopics.add(((Sink) node).topic());
        }
      }
    }
    return ImmutableSet.copyOf(usedTopics);
  }

  private static String getQueryApplicationId(
      final String serviceId,
      final String queryPrefix,
      final QueryId queryId) {
    return serviceId + queryPrefix + queryId;
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

  private static String addTimeSuffix(final String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }

  private Optional<MaterializationProvider> buildMaterializationProvider(
      final MaterializationInfo info,
      final KafkaStreams kafkaStreams,
      final PhysicalSchema schema,
      final KeyFormat keyFormat,
      final Map<String, Object> streamsProperties,
      final String applicationId
  ) {
    final Serializer<Struct> keySerializer = new GenericKeySerDe().create(
        keyFormat.getFormatInfo(),
        schema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).serializer();

    final Optional<KsMaterialization> ksMaterialization = ksMaterializationFactory
        .create(
            info.stateStoreName(),
            kafkaStreams,
            info.getStateStoreSchema(),
            keySerializer,
            keyFormat.getWindowInfo(),
            streamsProperties,
            ksqlConfig,
            applicationId
        );

    return ksMaterialization.map(ksMat -> (queryId, contextStacker) -> ksqlMaterializationFactory
        .create(
            ksMat,
            info,
            queryId,
            contextStacker
        ));
  }
}
