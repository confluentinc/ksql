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

package io.confluent.ksql.embedded;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.services.DefaultConnectClient;
import io.confluent.ksql.services.DefaultConnectClientFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class KsqlContextTestUtil {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private KsqlContextTestUtil() {
  }

  public static KsqlContext create(
      final KsqlConfig ksqlConfig,
      final SchemaRegistryClient schemaRegistryClient,
      final FunctionRegistry functionRegistry
  ) {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final Admin adminClient = clientSupplier
        .getAdmin(ksqlConfig.getKsqlAdminClientConfigProps());

    final KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(() -> adminClient);

    final ServiceContext serviceContext = TestServiceContext.create(
        clientSupplier,
        adminClient,
        kafkaTopicClient,
        () -> schemaRegistryClient,
        new DefaultConnectClientFactory(ksqlConfig)
            .get(Optional.empty(), Collections.emptyList(), Optional.empty())
    );

    final String metricsPrefix = "instance-" + COUNTER.getAndIncrement() + "-";

    final KsqlEngine engine = new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        functionRegistry,
        ServiceInfo.create(ksqlConfig, metricsPrefix),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        Collections.emptyList(),
        new MetricCollectors()
    );

    return new KsqlContext(
        serviceContext,
        ksqlConfig,
        engine,
        Injectors.DEFAULT
    );
  }
}
