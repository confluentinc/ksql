/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueuedSchemaKStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class PhysicalPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final Map<String, Object> overriddenProperties;
  private final MetaStore metaStore;
  private final boolean updateMetastore;
  private final QueryIdGenerator queryIdGenerator;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Consumer<QueryMetadata> queryCloseCallback;

  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> overriddenProperties,
      final boolean updateMetastore,
      final MetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Consumer<QueryMetadata> queryCloseCallback
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.overriddenProperties =
        Objects.requireNonNull(overriddenProperties, "overriddenProperties");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.updateMetastore = updateMetastore;
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder, "kafkaStreamsBuilder");
    this.queryCloseCallback = Objects.requireNonNull(queryCloseCallback, "queryCloseCallback");
  }

  public QueryMetadata buildPhysicalPlan(final LogicalPlanNode logicalPlanNode) {
    final SchemaKStream resultStream = logicalPlanNode
        .getNode()
        .buildStream(
            builder,
            ksqlConfig,
            serviceContext,
            functionRegistry,
            overriddenProperties
        );
    final OutputNode outputNode = resultStream.outputNode();
    final boolean isBareQuery = outputNode instanceof KsqlBareOutputNode;

    // Check to make sure the logical and physical plans match up;
    // important to do this BEFORE actually starting up
    // the corresponding Kafka Streams job
    if (isBareQuery && !(resultStream instanceof QueuedSchemaKStream)) {
      throw new KsqlException(String.format(
          "Mismatch between logical and physical output; "
          + "expected a QueuedSchemaKStream based on logical "
          + "KsqlBareOutputNode, found a %s instead",
          resultStream.getClass().getCanonicalName()
      ));
    }
    final String serviceId = getServiceId();
    final String persistanceQueryPrefix =
        ksqlConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);
    final String transientQueryPrefix =
        ksqlConfig.getString(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG);

    if (isBareQuery) {
      return buildPlanForBareQuery(
          (QueuedSchemaKStream) resultStream,
          (KsqlBareOutputNode) outputNode,
          serviceId,
          transientQueryPrefix,
          logicalPlanNode.getStatementText()
      );

    } else if (outputNode instanceof KsqlStructuredDataOutputNode) {

      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) outputNode;
      ksqlStructuredDataOutputNode = ksqlStructuredDataOutputNode.cloneWithDoCreateInto(
          ((KsqlStructuredDataOutputNode) logicalPlanNode.getNode()).isDoCreateInto()
      );
      return buildPlanForStructuredOutputNode(
          logicalPlanNode.getStatementText(),
          resultStream,
          ksqlStructuredDataOutputNode,
          serviceId,
          persistanceQueryPrefix,
          logicalPlanNode.getStatementText());


    } else {
      throw new KsqlException(
          "Sink data source of type: "
          + outputNode.getClass()
          + " is not supported.");
    }
  }

  private QueryMetadata buildPlanForBareQuery(
      final QueuedSchemaKStream<?> schemaKStream,
      final KsqlBareOutputNode bareOutputNode,
      final String serviceId,
      final String transientQueryPrefix,
      final String statement
  ) {

    final String applicationId = addTimeSuffix(getBareQueryApplicationId(
        serviceId,
        transientQueryPrefix
    ));

    final Map<String, Object> streamsProperties = buildStreamsProperties(
        applicationId,
        ksqlConfig,
        overriddenProperties
    );
    final KafkaStreams streams = kafkaStreamsBuilder.buildKafkaStreams(builder, streamsProperties);

    final SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

    return new QueuedQueryMetadata(
        statement,
        streams,
        bareOutputNode,
        schemaKStream.getExecutionPlan(""),
        schemaKStream.getQueue(),
        (sourceSchemaKstream instanceof SchemaKTable)
            ? DataSource.DataSourceType.KTABLE : DataSource.DataSourceType.KSTREAM,
        applicationId,
        builder.build(),
        streamsProperties,
        overriddenProperties,
        queryCloseCallback
    );
  }


  private QueryMetadata buildPlanForStructuredOutputNode(
      final String sqlExpression, final SchemaKStream<?> schemaKStream,
      final KsqlStructuredDataOutputNode outputNode,
      final String serviceId,
      final String persistanceQueryPrefix,
      final String statement
  ) {

    if (metaStore.getTopic(outputNode.getKsqlTopic().getName()) == null) {
      metaStore.putTopic(outputNode.getKsqlTopic());
    }
    final StructuredDataSource sinkDataSource;
    if (schemaKStream instanceof SchemaKTable) {
      final SchemaKTable<?> schemaKTable = (SchemaKTable) schemaKStream;
      sinkDataSource =
          new KsqlTable<>(
              sqlExpression,
              outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampExtractionPolicy(),
              outputNode.getKsqlTopic(),
              outputNode.getId().toString()
                  + ksqlConfig.getString(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG),
              schemaKTable.getKeySerde()
          );
    } else {
      sinkDataSource =
          new KsqlStream<>(
              sqlExpression,
              outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampExtractionPolicy(),
              outputNode.getKsqlTopic(),
              schemaKStream.getKeySerde()
          );

    }

    sinkSetUp(outputNode, sinkDataSource);

    final QueryId queryId;
    if (outputNode.isDoCreateInto()) {
      queryId = new QueryId(sinkDataSource.getPersistentQueryId().getId() + "_"
                            + queryIdGenerator.getNextId());
    } else {
      queryId = new QueryId("InsertQuery_" + queryIdGenerator.getNextId());
    }
    final String applicationId = serviceId + persistanceQueryPrefix + queryId;

    final Map<String, Object> streamsProperties = buildStreamsProperties(
        applicationId,
        ksqlConfig,
        overriddenProperties
    );
    final KafkaStreams streams = kafkaStreamsBuilder.buildKafkaStreams(builder, streamsProperties);

    final Topology topology = builder.build();

    return new PersistentQueryMetadata(
        statement,
        streams,
        outputNode,
        sinkDataSource,
        schemaKStream.getExecutionPlan(""),
        queryId,
        (schemaKStream instanceof SchemaKTable) ? DataSource.DataSourceType.KTABLE
                                                : DataSource.DataSourceType.KSTREAM,
        applicationId,
        sinkDataSource.getKsqlTopic(),
        topology,
        streamsProperties,
        overriddenProperties,
        queryCloseCallback
    );
  }

  private void sinkSetUp(final KsqlStructuredDataOutputNode outputNode,
                         final StructuredDataSource sinkDataSource) {
    if (outputNode.isDoCreateInto()) {
      if (updateMetastore) {
        metaStore.putSource(sinkDataSource.cloneWithTimeKeyColumns());
      }
    } else {
      final StructuredDataSource structuredDataSource =
          metaStore.getSource(sinkDataSource.getName());
      if (structuredDataSource.getDataSourceType() != sinkDataSource.getDataSourceType()) {
        throw new KsqlException(String.format("Incompatible data sink and query result. Data sink"
                                              + " (%s) type is %s but select query result is %s.",
                                              sinkDataSource.getName(),
                                              sinkDataSource.getDataSourceType(),
                                              structuredDataSource.getDataSourceType()));
      }
      final Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
          sinkDataSource.cloneWithTimeKeyColumns().getSchema());
      if (!SchemaUtil.areEqualSchemas(
          resultSchema,
          SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(structuredDataSource.getSchema()))) {
        throw new KsqlException(String.format("Incompatible schema between results and sink. "
                                              + "Result schema is %s, but the sink schema is %s"
                                              + ".",
                                              SchemaUtil.getSchemaDefinitionString(resultSchema),
                                              SchemaUtil.getSchemaDefinitionString(
                                                  SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
                                                      structuredDataSource.getSchema()))));
      }
      enforceKeyEquivalence(structuredDataSource.getKeyField(), sinkDataSource.getKeyField());

    }
  }

  private static String getBareQueryApplicationId(
      final String serviceId,
      final String transientQueryPrefix
  ) {
    return serviceId + transientQueryPrefix + Math.abs(ThreadLocalRandom.current().nextLong());
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

  private Map<String, Object> buildStreamsProperties(
      final String applicationId,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final Map<String, Object> newStreamsProperties
        = new HashMap<>(ksqlConfig.getKsqlStreamConfigProps());
    newStreamsProperties.putAll(overriddenProperties);
    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

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

  private static void enforceKeyEquivalence(final Field sinkKeyField, final Field resultKeyField) {
    if (sinkKeyField == null && resultKeyField == null) {
      return;
    }

    if (sinkKeyField != null
        && resultKeyField != null
        && sinkKeyField.name().equalsIgnoreCase(resultKeyField.name())
        && Objects.equals(sinkKeyField.schema(), resultKeyField.schema())) {
      return;
    }

    throwIncompatibleKeysException(sinkKeyField, resultKeyField);
  }

  private static void throwIncompatibleKeysException(
      final Field sinkKeyField,
      final Field resultKeyField
  ) {
    throw new KsqlException(String.format(
        "Incompatible key fields for sink and results. Sink"
            + " key field is %s (type: %s) while result key "
            + "field is %s (type: %s)",
        sinkKeyField == null ? null : sinkKeyField.name(),
        sinkKeyField == null ? null : sinkKeyField.schema().toString(),
        resultKeyField == null ? null : resultKeyField.name(),
        resultKeyField == null ? null : resultKeyField.schema().toString()));
  }

  // Package private because of test
  String getServiceId() {
    return KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
           + ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }
}

