/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.physical;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.QueuedSchemaKStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.KsqlTimestampExtractor;

public class PhysicalPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final KafkaTopicClient kafkaTopicClient;
  private final MetastoreUtil metastoreUtil;
  private final FunctionRegistry functionRegistry;
  private final Map<String, Object> overriddenStreamsProperties;
  private final MetaStore metaStore;
  private final boolean updateMetastore;
  private final SchemaRegistryClient schemaRegistryClient;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;

  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final MetastoreUtil metastoreUtil,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> overriddenStreamsProperties,
      final boolean updateMetastore,
      final MetaStore metaStore,
      final SchemaRegistryClient schemaRegistryClient,
      final KafkaStreamsBuilder kafkaStreamsBuilder
  ) {
    this.builder = builder;
    this.ksqlConfig = ksqlConfig;
    this.kafkaTopicClient = kafkaTopicClient;
    this.metastoreUtil = metastoreUtil;
    this.functionRegistry = functionRegistry;
    this.overriddenStreamsProperties = overriddenStreamsProperties;
    this.metaStore = metaStore;
    this.updateMetastore = updateMetastore;
    this.schemaRegistryClient = schemaRegistryClient;
    this.kafkaStreamsBuilder = kafkaStreamsBuilder;
  }

  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final MetastoreUtil metastoreUtil,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> overriddenStreamsProperties,
      final boolean updateMetastore,
      final MetaStore metaStore,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    this(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        metastoreUtil,
        functionRegistry,
        overriddenStreamsProperties,
        updateMetastore,
        metaStore,
        schemaRegistryClient,
        new KafkaStreamsBuilderImpl()
    );
  }


  public QueryMetadata buildPhysicalPlan(final Pair<String, PlanNode> statementPlanPair)
      throws Exception {
    final SchemaKStream resultStream = statementPlanPair
        .getRight()
        .buildStream(
            builder,
            ksqlConfig,
            kafkaTopicClient,
            metastoreUtil,
            functionRegistry,
            overriddenStreamsProperties,
            schemaRegistryClient
        );
    final OutputNode outputNode = resultStream.outputNode();
    boolean isBareQuery = outputNode instanceof KsqlBareOutputNode;

    // Check to make sure the logical and physical plans match up;
    // important to do this BEFORE actually starting up
    // the corresponding Kafka Streams job
    if (isBareQuery && !(resultStream instanceof QueuedSchemaKStream)) {
      throw new Exception(String.format(
          "Mismatch between logical and physical output; "
          + "expected a QueuedSchemaKStream based on logical "
          + "KsqlBareOutputNode, found a %s instead",
          resultStream.getClass().getCanonicalName()
      ));
    }
    String serviceId = getServiceId();
    String
        persistanceQueryPrefix =
        ksqlConfig.get(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG).toString();
    String
        transientQueryPrefix =
        ksqlConfig.get(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG).toString();

    if (isBareQuery) {

      return buildPlanForBareQuery(
          (QueuedSchemaKStream) resultStream,
          (KsqlBareOutputNode) outputNode,
          serviceId,
          transientQueryPrefix,
          statementPlanPair.getLeft()
      );

    } else if (outputNode instanceof KsqlStructuredDataOutputNode) {

      return buildPlanForStructuredOutputNode(
          statementPlanPair.getLeft(),
          resultStream,
          (KsqlStructuredDataOutputNode) outputNode,
          serviceId,
          persistanceQueryPrefix,
          statementPlanPair.getLeft()
      );

    } else {
      throw new KsqlException(
          "Sink data source of type: "
          + outputNode.getClass()
          + " is not supported.");
    }
  }

  private QueryMetadata buildPlanForBareQuery(
      final QueuedSchemaKStream schemaKStream,
      final KsqlBareOutputNode bareOutputNode,
      final String serviceId,
      final String transientQueryPrefix,
      final String statement
  ) {

    final String applicationId = addTimeSuffix(getBareQueryApplicationId(
        serviceId,
        transientQueryPrefix
    ));

    KafkaStreams streams = buildStreams(
        builder,
        applicationId,
        ksqlConfig,
        overriddenStreamsProperties
    );

    SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

    return new QueuedQueryMetadata(
        statement,
        streams,
        bareOutputNode,
        schemaKStream.getExecutionPlan(""),
        schemaKStream.getQueue(),
        (sourceSchemaKstream instanceof SchemaKTable)
            ? DataSource.DataSourceType.KTABLE : DataSource.DataSourceType.KSTREAM,
        applicationId,
        kafkaTopicClient,
        builder.build()
    );
  }


  private QueryMetadata buildPlanForStructuredOutputNode(
      String sqlExpression, final SchemaKStream schemaKStream,
      final KsqlStructuredDataOutputNode outputNode,
      final String serviceId,
      final String persistanceQueryPrefix,
      final String statement
  ) {

    if (metaStore.getTopic(outputNode.getKafkaTopicName()) == null) {
      metaStore.putTopic(outputNode.getKsqlTopic());
    }
    StructuredDataSource sinkDataSource;
    if (schemaKStream instanceof SchemaKTable) {
      SchemaKTable schemaKTable = (SchemaKTable) schemaKStream;
      sinkDataSource =
          new KsqlTable(
              sqlExpression,
              outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampField(),
              outputNode.getKsqlTopic(),
              outputNode.getId().toString()
                  + ksqlConfig.get(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG),
              schemaKTable.isWindowed()
          );
    } else {
      sinkDataSource =
          new KsqlStream(
              sqlExpression,
              outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampField(),
              outputNode.getKsqlTopic()
          );

    }

    if (updateMetastore) {
      metaStore.putSource(sinkDataSource.cloneWithTimeKeyColumns());
    }

    final QueryId queryId = sinkDataSource.getPersistentQueryId();
    final String applicationId = serviceId + persistanceQueryPrefix + queryId;

    KafkaStreams streams = buildStreams(
        builder,
        applicationId,
        ksqlConfig,
        overriddenStreamsProperties
    );

    Topology topology = builder.build();

    return new PersistentQueryMetadata(
        statement,
        streams, outputNode, schemaKStream
            .getExecutionPlan(""), queryId,
        (schemaKStream instanceof SchemaKTable)
            ? DataSource.DataSourceType.KTABLE
            : DataSource.DataSourceType.KSTREAM,
        applicationId,
        kafkaTopicClient,
        outputNode.getSchema(),
        sinkDataSource.getKsqlTopic(),
        topology
    );
  }

  private String getBareQueryApplicationId(String serviceId, String transientQueryPrefix) {
    return serviceId + transientQueryPrefix + Math.abs(ThreadLocalRandom.current().nextLong());
  }

  private String addTimeSuffix(String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }

  private void updateListProperty(Map<String, Object> properties, String key, Object value) {
    Object obj = properties.getOrDefault(key, new LinkedList<String>());
    List valueList;
    // The property value is either a comma-separated string of class names, or a list of class
    // names
    if (obj instanceof String) {
      // If its a string just split it on the separator so we dont have to worry about adding a
      // separator
      String asString = (String) obj;
      valueList = new LinkedList<>(Arrays.asList(asString.split("\\s*,\\s*")));
    } else if (obj instanceof List) {
      valueList = (List) obj;
    } else {
      throw new KsqlException("Expecting list or string for property: " + key);
    }
    valueList.add(value);
    properties.put(key, valueList);
  }

  private KafkaStreams buildStreams(
      final StreamsBuilder builder,
      final String applicationId,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    Map<String, Object> newStreamsProperties = ksqlConfig.getKsqlStreamConfigProps();
    newStreamsProperties.putAll(overriddenProperties);
    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    newStreamsProperties.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ksqlConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
    );
    newStreamsProperties.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        ksqlConfig.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
    );
    newStreamsProperties.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        ksqlConfig.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG)
    );
    if (ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX) != null) {
      newStreamsProperties.put(
          KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX,
          ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX)
      );
      newStreamsProperties.put(
          StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, KsqlTimestampExtractor.class);
    }
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
    return kafkaStreamsBuilder.buildKafkaStreams(builder, new StreamsConfig(newStreamsProperties));
  }

  // Package private because of test
  String getServiceId() {
    return KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
           + ksqlConfig.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG).toString();
  }
}

