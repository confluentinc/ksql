/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.physical;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.materialization.KsqlMaterializationFactory;
import io.confluent.ksql.materialization.MaterializationInfo;
import io.confluent.ksql.materialization.MaterializationProvider;
import io.confluent.ksql.materialization.ks.KsMaterialization;
import io.confluent.ksql.materialization.ks.KsMaterializationFactory;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QuerySchemas;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class PhysicalPlanBuilder {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final Map<String, Object> overriddenProperties;
  private final MutableMetaStore metaStore;
  private final QueryIdGenerator queryIdGenerator;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Consumer<QueryMetadata> queryCloseCallback;
  private final KsMaterializationFactory ksMaterializationFactory;
  private final KsqlMaterializationFactory ksqlMaterializationFactory;


  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> overriddenProperties,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Consumer<QueryMetadata> queryCloseCallback
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.overriddenProperties =
        Objects.requireNonNull(overriddenProperties, "overriddenProperties");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder, "kafkaStreamsBuilder");
    this.queryCloseCallback = Objects.requireNonNull(queryCloseCallback, "queryCloseCallback");
    this.ksMaterializationFactory = new KsMaterializationFactory();
    this.ksqlMaterializationFactory = new KsqlMaterializationFactory(
        ksqlConfig,
        functionRegistry,
        processingLogContext
    );
  }

  public QueryMetadata buildPhysicalPlan(final LogicalPlanNode logicalPlanNode) {
    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    final QueryId queryId = outputNode.getQueryId(queryIdGenerator, logicalPlanNode.getOffset());

    final KsqlQueryBuilder ksqlQueryBuilder = KsqlQueryBuilder.of(
        builder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );

    final SchemaKStream<?> resultStream = outputNode.buildStream(ksqlQueryBuilder);

    if (outputNode instanceof KsqlBareOutputNode) {
      final String transientQueryPrefix =
          ksqlConfig.getString(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG);

      return buildPlanForBareQuery(
          resultStream,
          (KsqlBareOutputNode) outputNode,
          getServiceId(),
          transientQueryPrefix,
          queryId,
          logicalPlanNode.getStatementText()
      );
    }

    if (outputNode instanceof KsqlStructuredDataOutputNode) {
      final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) outputNode;

      final String persistanceQueryPrefix =
          ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);

      return buildPlanForStructuredOutputNode(
          logicalPlanNode.getStatementText(),
          resultStream,
          ksqlStructuredDataOutputNode,
          getServiceId(),
          persistanceQueryPrefix,
          queryId,
          ksqlQueryBuilder.getSchemas()
      );
    }

    throw new KsqlException("Sink data source type unsupported: " + outputNode.getClass());
  }

  private QueryMetadata buildPlanForBareQuery(
      final SchemaKStream<?> schemaKStream,
      final KsqlBareOutputNode bareOutputNode,
      final String serviceId,
      final String transientQueryPrefix,
      final QueryId queryId,
      final String statement
  ) {

    final String applicationId = addTimeSuffix(getQueryApplicationId(
        serviceId,
        transientQueryPrefix,
        queryId
    ));

    final Map<String, Object> streamsProperties = buildStreamsProperties(
        applicationId,
        ksqlConfig,
        queryId,
        processingLogContext
    );

    final TransientQueryQueue<?> queue =
        new TransientQueryQueue<>(schemaKStream, bareOutputNode.getLimit());

    final KafkaStreams streams = kafkaStreamsBuilder.buildKafkaStreams(builder, streamsProperties);

    final SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

    return new TransientQueryMetadata(
        statement,
        streams,
        bareOutputNode.getSchema(),
        getSourceNames(bareOutputNode),
        queue::setLimitHandler,
        schemaKStream.getExecutionPlan(""),
        queue.getQueue(),
        (sourceSchemaKstream instanceof SchemaKTable)
            ? DataSourceType.KTABLE
            : DataSourceType.KSTREAM,
        applicationId,
        builder.build(),
        streamsProperties,
        overriddenProperties,
        queryCloseCallback
    );
  }

  private QueryMetadata buildPlanForStructuredOutputNode(
      final String sqlExpression,
      final SchemaKStream<?> schemaKStream,
      final KsqlStructuredDataOutputNode outputNode,
      final String serviceId,
      final String persistanceQueryPrefix,
      final QueryId queryId,
      final QuerySchemas schemas
  ) {
    final DataSourceType sourceType = (schemaKStream instanceof SchemaKTable)
        ? DataSourceType.KTABLE
        : DataSourceType.KSTREAM;

    final DataSource<?> sinkDataSource;
    if (sourceType == DataSourceType.KTABLE) {
      sinkDataSource = new KsqlTable<>(
          sqlExpression,
          outputNode.getId().toString(),
          outputNode.getSchema(),
          outputNode.getSerdeOptions(),
          schemaKStream.getKeyField(),
          outputNode.getTimestampExtractionPolicy(),
          outputNode.getKsqlTopic()
      );
    } else {
      sinkDataSource = new KsqlStream<>(
          sqlExpression,
          outputNode.getId().toString(),
          outputNode.getSchema(),
          outputNode.getSerdeOptions(),
          schemaKStream.getKeyField(),
          outputNode.getTimestampExtractionPolicy(),
          outputNode.getKsqlTopic()
      );
    }

    sinkSetUp(outputNode, sinkDataSource);

    final String applicationId = getQueryApplicationId(
        serviceId,
        persistanceQueryPrefix,
        queryId
    );

    final Map<String, Object> streamsProperties = buildStreamsProperties(
        applicationId,
        ksqlConfig,
        queryId,
        processingLogContext
    );

    final KafkaStreams streams = kafkaStreamsBuilder.buildKafkaStreams(builder, streamsProperties);

    final Topology topology = builder.build();

    final PhysicalSchema querySchema = PhysicalSchema
        .from(outputNode.getSchema(), outputNode.getSerdeOptions());

    final Optional<MaterializationInfo> materializationInfo = sourceType == DataSourceType.KTABLE
        ? findMaterializationInfo(outputNode)
        : Optional.empty();

    final Optional<MaterializationProvider> materializationBuilder = materializationInfo
        .flatMap(info -> buildMaterializationProvider(
            info,
            streams,
            querySchema,
            sinkDataSource.getKsqlTopic().getKeyFormat(),
            streamsProperties
        ));

    return new PersistentQueryMetadata(
        sqlExpression,
        streams,
        querySchema,
        getSourceNames(outputNode),
        sinkDataSource.getName(),
        schemaKStream.getExecutionPlan(""),
        queryId,
        sourceType,
        materializationBuilder,
        applicationId,
        sinkDataSource.getKsqlTopic(),
        topology,
        schemas,
        streamsProperties,
        overriddenProperties,
        queryCloseCallback
    );
  }

  private static Optional<MaterializationInfo> findMaterializationInfo(
      final PlanNode node
  ) {
    if (node instanceof AggregateNode) {
      return ((AggregateNode) node).getMaterializationInfo();
    }

    return node.getSources().stream()
        .map(PhysicalPlanBuilder::findMaterializationInfo)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  private Optional<MaterializationProvider> buildMaterializationProvider(
      final MaterializationInfo info,
      final KafkaStreams kafkaStreams,
      final PhysicalSchema schema,
      final KeyFormat keyFormat,
      final Map<String, Object> streamsProperties
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
            keySerializer,
            keyFormat.getWindowType(),
            streamsProperties
        );

    return ksMaterialization.map(ksMat -> contextStacker -> ksqlMaterializationFactory
        .create(
            ksMat,
            info,
            contextStacker
        ));
  }

  private void sinkSetUp(
      final KsqlStructuredDataOutputNode outputNode,
      final DataSource<?> sinkDataSource
  ) {
    if (outputNode.isDoCreateInto()) {
      metaStore.putSource(sinkDataSource);
      return;
    }

    final DataSource<?> existing =
        metaStore.getSource(sinkDataSource.getName());

    if (existing.getDataSourceType() != sinkDataSource.getDataSourceType()) {
      throw new KsqlException(String.format("Incompatible data sink and query result. Data sink"
              + " (%s) type is %s but select query result is %s.",
          sinkDataSource.getName(),
          sinkDataSource.getDataSourceType(),
          existing.getDataSourceType()));
    }

    final LogicalSchema resultSchema = sinkDataSource.getSchema();
    final LogicalSchema existingSchema = existing.getSchema();

    if (!resultSchema.equals(existingSchema)) {
      throw new KsqlException("Incompatible schema between results and sink. "
          + "Result schema is " + resultSchema
          + ", but the sink schema is " + existingSchema + ".");
    }

    enforceKeyEquivalence(
        existing.getKeyField().resolve(existingSchema, ksqlConfig),
        sinkDataSource.getKeyField().resolve(resultSchema, ksqlConfig)
    );
  }

  private static String getQueryApplicationId(
      final String serviceId,
      final String queryPrefix,
      final QueryId queryId) {
    return serviceId + queryPrefix + queryId;
  }

  private static String addTimeSuffix(final String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }

  @SuppressWarnings("unchecked")
  private static void updateListProperty(
      final Map<String, Object> properties,
      final String key,
      final Object value
  ) {
    final Object obj = properties.getOrDefault(key, new LinkedList<String>());
    final List valueList;
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
      valueList = new LinkedList<>((List) obj);
    } else {
      throw new KsqlException("Expecting list or string for property: " + key);
    }
    valueList.add(value);
    properties.put(key, valueList);
  }

  private static Map<String, Object> buildStreamsProperties(
      final String applicationId,
      final KsqlConfig ksqlConfig,
      final QueryId queryId,
      final ProcessingLogContext processingLogContext
  ) {
    final Map<String, Object> newStreamsProperties
        = new HashMap<>(ksqlConfig.getKsqlStreamConfigProps());
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

  private static void enforceKeyEquivalence(
      final Optional<Column> sinkKeyCol,
      final Optional<Column> resultKeyCol
  ) {
    if (!sinkKeyCol.isPresent() && !resultKeyCol.isPresent()) {
      return;
    }

    if (sinkKeyCol.isPresent()
        && resultKeyCol.isPresent()
        && sinkKeyCol.get().name().equalsIgnoreCase(resultKeyCol.get().name())
        && Objects.equals(sinkKeyCol.get().type(), resultKeyCol.get().type())) {
      return;
    }

    throwIncompatibleKeysException(sinkKeyCol, resultKeyCol);
  }

  private static void throwIncompatibleKeysException(
      final Optional<Column> sinkKeyCol,
      final Optional<Column> resultKeyCol
  ) {
    throw new KsqlException(String.format(
        "Incompatible key fields for sink and results. Sink"
            + " key field is %s (type: %s) while result key "
            + "field is %s (type: %s)",
        sinkKeyCol.map(Column::name).orElse(null),
        sinkKeyCol.map(Column::type).orElse(null),
        resultKeyCol.map(Column::name).orElse(null),
        resultKeyCol.map(Column::type).orElse(null)));
  }

  // Package private because of test
  String getServiceId() {
    return KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
           + ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }

  private static Set<String> getSourceNames(final PlanNode outputNode) {
    final PlanSourceExtractorVisitor<?, ?> visitor = new PlanSourceExtractorVisitor<>();
    visitor.process(outputNode, null);
    return visitor.getSourceNames();
  }
}

