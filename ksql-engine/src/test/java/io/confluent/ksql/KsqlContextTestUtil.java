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

package io.confluent.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.inference.DefaultSchemaInjector;
import io.confluent.ksql.topic.DefaultTopicInjector;
import io.confluent.ksql.schema.inference.SchemaRegistryTopicSchemaSupplier;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.test.TestUtils;

public final class KsqlContextTestUtil {

  private KsqlContextTestUtil() {
  }

  public static KsqlContext create(
      final KsqlConfig ksqlConfig,
      final SchemaRegistryClient schemaRegistryClient,
      final FunctionRegistry functionRegistry
  ) {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final AdminClient adminClient = clientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());

    final KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);

    final ServiceContext serviceContext = TestServiceContext.create(
        clientSupplier,
        adminClient,
        kafkaTopicClient,
        () -> schemaRegistryClient
    );

    final KsqlEngine engine = new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        functionRegistry,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
    );

    final DefaultSchemaInjector schemaInjector = new DefaultSchemaInjector(
        new SchemaRegistryTopicSchemaSupplier(serviceContext.getSchemaRegistryClient()));
    return new KsqlContext(
        serviceContext, ksqlConfig, engine, sc -> schemaInjector, DefaultTopicInjector::new);
  }

  public static KsqlConfig createKsqlConfig(final EmbeddedSingleNodeKafkaCluster kafkaCluster) {
    return createKsqlConfig(kafkaCluster, Collections.emptyMap());
  }

  public static KsqlConfig createKsqlConfig(
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final Map<String, Object> additionalConfig
  ) {
    final ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
        .putAll(kafkaCluster.getClientProperties())
        .putAll(additionalConfig)
        .build();
    return createKsqlConfig(kafkaCluster.bootstrapServers(), config);
  }

  public static KsqlConfig createKsqlConfig(
      final String kafkaBootstrapServers,
      final Map<String, Object> additionalConfig
  ) {
    final Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    config.put("commit.interval.ms", 0);
    config.put("cache.max.bytes.buffering", 0);
    config.put("auto.offset.reset", "earliest");
    config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    config.putAll(additionalConfig);
    return new KsqlConfig(config);
  }
}
