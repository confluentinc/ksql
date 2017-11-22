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

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.QueuedSchemaKStream;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.timestamp.KsqlTimestampExtractor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class PhysicalPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final KafkaTopicClient kafkaTopicClient;
  private final MetastoreUtil metastoreUtil;
  private final FunctionRegistry functionRegistry;
  private final boolean addUniqueTimeSuffix;
  private final Map<String, Object> overriddenStreamsProperties;
  private final MetaStore metaStore;
  private final boolean updateMetastore;
  private final AtomicLong queryIdCounter;

  public PhysicalPlanBuilder(final StreamsBuilder builder,
                             final KsqlConfig ksqlConfig,
                             final KafkaTopicClient kafkaTopicClient,
                             final MetastoreUtil metastoreUtil,
                             final FunctionRegistry functionRegistry,
                             final boolean addUniqueTimeSuffix,
                             final Map<String, Object> overriddenStreamsProperties,
                             final boolean updateMetastore,
                             final MetaStore metaStore,
                             final AtomicLong queryIdCounter) {
    this.builder = builder;
    this.ksqlConfig = ksqlConfig;
    this.kafkaTopicClient = kafkaTopicClient;
    this.metastoreUtil = metastoreUtil;
    this.functionRegistry = functionRegistry;
    this.addUniqueTimeSuffix = addUniqueTimeSuffix;
    this.overriddenStreamsProperties = overriddenStreamsProperties;
    this.metaStore = metaStore;
    this.updateMetastore = updateMetastore;
    this.queryIdCounter = queryIdCounter;
  }

  public QueryMetadata buildPhysicalPlan(final Pair<String, PlanNode> statementPlanPair) throws Exception {
    final SchemaKStream resultStream = statementPlanPair.getRight().buildStream(builder,
        ksqlConfig,
        kafkaTopicClient,
        metastoreUtil,
        functionRegistry,
        new HashMap<>());
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
    String serviceId = ksqlConfig.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG).toString();
    String persistanceQueryPrefix = ksqlConfig.get(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG).toString();
    String transientQueryPrefix = ksqlConfig.get(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG).toString();

    if (isBareQuery) {

      return buildPlanForBareQuery((QueuedSchemaKStream) resultStream, (KsqlBareOutputNode) outputNode,
          serviceId, transientQueryPrefix, statementPlanPair.getLeft());

    } else if (outputNode instanceof KsqlStructuredDataOutputNode) {

      return buildPlanForStructuredOutputNode(resultStream,
          (KsqlStructuredDataOutputNode) outputNode, serviceId, persistanceQueryPrefix, statementPlanPair.getLeft());

    } else {
      throw new KsqlException("Sink data source of type: " + outputNode.getClass() + " is not supported.");
    }
  }

  private QueryMetadata buildPlanForBareQuery(final QueuedSchemaKStream schemaKStream,
                                              final KsqlBareOutputNode bareOutputNode,
                                              final String serviceId,
                                              final String transientQueryPrefix,
                                              final String statement) {

    String applicationId = getBareQueryApplicationId(serviceId, transientQueryPrefix);
    if (addUniqueTimeSuffix) {
      applicationId = addTimeSuffix(applicationId);
    }

    KafkaStreams streams = buildStreams(builder, applicationId, ksqlConfig, overriddenStreamsProperties);

    SchemaKStream sourceSchemaKstream = schemaKStream.getSourceSchemaKStreams().get(0);

    return new QueuedQueryMetadata(
        statement,
        streams,
        bareOutputNode,
        schemaKStream.getExecutionPlan(""),
        schemaKStream.getQueue(),
        (sourceSchemaKstream instanceof SchemaKTable) ?
            DataSource.DataSourceType.KTABLE : DataSource.DataSourceType.KSTREAM,
        applicationId,
        kafkaTopicClient,
        ksqlConfig
    );
  }


  private QueryMetadata buildPlanForStructuredOutputNode(final SchemaKStream schemaKStream,
                                                         final KsqlStructuredDataOutputNode outputNode,
                                                         final String serviceId,
                                                         final String persistanceQueryPrefix,
                                                         final String statement) {

    long queryId = queryIdCounter.getAndIncrement();
    String applicationId = serviceId + persistanceQueryPrefix + queryId;
    if (addUniqueTimeSuffix) {
      applicationId = addTimeSuffix(applicationId);
    }

    if (metaStore.getTopic(outputNode.getKafkaTopicName()) == null) {
      metaStore.putTopic(outputNode.getKsqlTopic());
    }
    StructuredDataSource sinkDataSource;
    if (schemaKStream instanceof SchemaKTable) {
      SchemaKTable schemaKTable = (SchemaKTable) schemaKStream;
      sinkDataSource =
          new KsqlTable(outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampField(),
              outputNode.getKsqlTopic(),
              outputNode.getId().toString() +
                  ksqlConfig.get(KsqlConfig.KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG),
              schemaKTable.isWindowed());
    } else {
      sinkDataSource =
          new KsqlStream(outputNode.getId().toString(),
              outputNode.getSchema(),
              schemaKStream.getKeyField(),
              outputNode.getTimestampField(),
              outputNode.getKsqlTopic());
    }

    if (updateMetastore) {
      metaStore.putSource(sinkDataSource.cloneWithTimeKeyColumns());
    }
    KafkaStreams streams = buildStreams(builder, applicationId, ksqlConfig, overriddenStreamsProperties);

    return new PersistentQueryMetadata(statement,
        streams, outputNode, schemaKStream
        .getExecutionPlan(""), queryId,
        (schemaKStream instanceof SchemaKTable) ? DataSource
            .DataSourceType.KTABLE : DataSource.DataSourceType
            .KSTREAM,
        applicationId,
        kafkaTopicClient,
        ksqlConfig);
  }

  private String getBareQueryApplicationId(String serviceId, String transientQueryPrefix) {
    return  serviceId + transientQueryPrefix +
        Math.abs(ThreadLocalRandom.current().nextLong());
  }

  private String addTimeSuffix(String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
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
        ksqlConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    newStreamsProperties.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        ksqlConfig.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
    newStreamsProperties.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        ksqlConfig.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
    if (ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX) != null) {
      newStreamsProperties.put(
          KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX,
          ksqlConfig.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX));
      newStreamsProperties.put(
          StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, KsqlTimestampExtractor.class);
    }
    return new KafkaStreams(builder.build(), new StreamsConfig(newStreamsProperties));
  }

}

