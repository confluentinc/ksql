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

package io.confluent.ksql.test.tools;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

final class TestExecutor {

  private final ServiceContext serviceContext = getServiceContext();
  private final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext);
  private final Map<String, Object> config = getConfigs(new HashMap<>());

  private final FakeKafkaService fakeKafkaService = FakeKafkaService.create();

  void buildAndExecuteQuery(final TestCase testCase) throws IOException, RestClientException {

    final KsqlConfig currentConfigs = new KsqlConfig(config);

    final Map<String, String> persistedConfigs = testCase.persistedProperties();

    final KsqlConfig ksqlConfig = persistedConfigs.isEmpty() ? currentConfigs :
        currentConfigs.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    try {
      final List<TopologyTestDriverContainer> topologyTestDrivers =
          TestExecutorUtil.buildStreamsTopologyTestDrivers(
              testCase,
              serviceContext,
              ksqlEngine,
              ksqlConfig,
              fakeKafkaService
      );

      writeInputIntoTopics(testCase.getInputRecords(), fakeKafkaService);
      final Set<String> inputTopics = testCase.getInputRecords()
          .stream()
          .map(record -> record.topic.getName())
          .collect(Collectors.toSet());

      for (final TopologyTestDriverContainer topologyTestDriverContainer : topologyTestDrivers) {
        testCase.verifyTopology();

        final Set<Topic> topicsFromInput = topologyTestDriverContainer.getSourceTopics()
            .stream()
            .filter(topic -> inputTopics.contains(topic.getName()))
            .collect(Collectors.toSet());
        final Set<Topic> topicsFromKafka = topologyTestDriverContainer.getSourceTopics()
            .stream()
            .filter(topic -> !inputTopics.contains(topic.getName()))
            .collect(Collectors.toSet());
        if (!topicsFromInput.isEmpty()) {
          pipeRecordsFromProvidedInput(
              testCase,
              fakeKafkaService,
              topologyTestDriverContainer,
              serviceContext);
        }
        for (final Topic kafkaTopic : topicsFromKafka) {
          pipeRecordsFromKafka(
              kafkaTopic.getName(),
              fakeKafkaService,
              topologyTestDriverContainer,
              serviceContext);
        }
      }
      testCase.verifyOutputTopics(fakeKafkaService);
      testCase.verifyMetastore(ksqlEngine.getMetaStore());
    } catch (final RuntimeException e) {
      testCase.handleException(e);
    }
  }

  void close() {
    serviceContext.close();
    ksqlEngine.close();
  }

  private static void pipeRecordsFromProvidedInput(
      final TestCase testCase,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer topologyTestDriverContainer,
      final ServiceContext serviceContext
  ) {

    for (final Record record : testCase.getInputRecords()) {
      if (topologyTestDriverContainer.getSourceTopicNames().contains(record.topic.getName())) {
        TestCase.processSingleRecord(
            FakeKafkaRecord.of(record, null),
            fakeKafkaService,
            topologyTestDriverContainer,
            serviceContext.getSchemaRegistryClient()
        );
      }
    }
  }

  private static void pipeRecordsFromKafka(
      final String kafkaTopicName,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer topologyTestDriverContainer,
      final ServiceContext serviceContext
  ) {
    for (final FakeKafkaRecord fakeKafkaRecord : fakeKafkaService
        .readRecords(kafkaTopicName)) {
      TestCase.processSingleRecord(
          fakeKafkaRecord,
          fakeKafkaService,
          topologyTestDriverContainer,
          serviceContext.getSchemaRegistryClient()
      );
    }
  }

  static ServiceContext getServiceContext() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    return TestServiceContext.create(() -> schemaRegistryClient);
  }

  static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
    final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    return KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
  }

  static Map<String, Object> getConfigs(final Map<String, Object> additionalConfigs) {

    final ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
        .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "some.ksql.service.id")
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
        .put(
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
        .put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");

    if (additionalConfigs != null) {
      mapBuilder.putAll(additionalConfigs);
    }
    return mapBuilder.build();

  }

  private static void writeInputIntoTopics(
      final List<Record> inputRecords,
      final FakeKafkaService fakeKafkaService
  ) {
    inputRecords.forEach(
        record -> fakeKafkaService.writeRecord(
            record.topic.getName(),
            FakeKafkaRecord.of(record, null))
    );
  }

}
