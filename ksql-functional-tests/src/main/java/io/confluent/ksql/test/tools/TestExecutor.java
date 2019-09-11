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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class TestExecutor implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final ServiceContext serviceContext;
  private final KsqlEngine ksqlEngine;
  private final Map<String, Object> config = getConfigs(new HashMap<>());
  private final FakeKafkaService fakeKafkaService;
  private final TopologyBuilder topologyBuilder;

  public TestExecutor() {
    this(FakeKafkaService.create(), getServiceContext());
  }

  @VisibleForTesting
  TestExecutor(
      final FakeKafkaService fakeKafkaService,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final TopologyBuilder topologyBuilder
  ) {
    this.fakeKafkaService = Objects.requireNonNull(fakeKafkaService, "fakeKafkaService");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.topologyBuilder = Objects.requireNonNull(topologyBuilder, "topologyBuilder");
  }

  private TestExecutor(
      final FakeKafkaService fakeKafkaService,
      final ServiceContext serviceContext
  ) {
    this(
        fakeKafkaService,
        serviceContext,
        getKsqlEngine(serviceContext),
        TestExecutorUtil::buildStreamsTopologyTestDrivers
    );
  }

  public void buildAndExecuteQuery(final TestCase testCase) {

    final KsqlConfig currentConfigs = new KsqlConfig(config);

    final Map<String, String> persistedConfigs = testCase.persistedProperties();

    final KsqlConfig ksqlConfig = persistedConfigs.isEmpty() ? currentConfigs :
        currentConfigs.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    try {
      final List<TopologyTestDriverContainer> topologyTestDrivers = topologyBuilder
          .buildStreamsTopologyTestDrivers(
              testCase,
              serviceContext,
              ksqlEngine,
              ksqlConfig,
              fakeKafkaService
          );

      testCase.expectedException().map(ee -> {
        throw new AssertionError("Expected test to throw" + StringDescription.toString(ee));
      });

      writeInputIntoTopics(testCase.getInputRecords(), fakeKafkaService);
      final Set<String> inputTopics = testCase.getInputRecords()
          .stream()
          .map(record -> record.topic.getName())
          .collect(Collectors.toSet());

      for (final TopologyTestDriverContainer topologyTestDriverContainer : topologyTestDrivers) {
        verifyTopology(testCase);

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
              testCase,
              kafkaTopic.getName(),
              fakeKafkaService,
              topologyTestDriverContainer,
              serviceContext);
        }
      }
      verifyOutput(testCase);
      testCase.getPostConditions().verify(ksqlEngine.getMetaStore());
    } catch (final RuntimeException e) {
      final Optional<Matcher<Throwable>> expectedExceptionMatcher = testCase.expectedException();
      if (!expectedExceptionMatcher.isPresent()) {
        throw e;
      }

      assertThat(e, isThrowable(expectedExceptionMatcher.get()));
    }
  }

  public void close() {
    serviceContext.close();
    ksqlEngine.close();
  }

  private void verifyOutput(final TestCase testCase) {
    final boolean ranWithInsertStatements = testCase.getInputRecords().size() == 0;

    final Map<String, List<Record>> expectedByTopic = testCase.getOutputRecords().stream()
        .collect(Collectors.groupingBy(r -> r.topic().getName()));

    final Map<String, List<FakeKafkaRecord>> actualByTopic = expectedByTopic.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), fakeKafkaService::readRecords));

    expectedByTopic.forEach((kafkaTopic, expectedRecords) ->
        validateTopicData(
            kafkaTopic,
            expectedRecords,
            actualByTopic.getOrDefault(kafkaTopic, ImmutableList.of()),
            ranWithInsertStatements
        ));
  }

  private static void validateTopicData(
      final String topicName,
      final List<Record> expected,
      final List<FakeKafkaRecord> actual,
      final boolean ranWithInsertStatements
  ) {
    if (actual.size() != expected.size()) {
      throw new KsqlException("Expected <" + expected.size()
          + "> records but it was <" + actual.size() + ">\n" + getActualsForErrorMessage(actual));
    }

    for (int i = 0; i < expected.size(); i++) {
      final Record expectedRecord = expected.get(i);
      final ProducerRecord<?, ?> actualProducerRecord = actual.get(i).getProducerRecord();

      validateCreatedMessage(
          topicName,
          expectedRecord,
          actualProducerRecord,
          ranWithInsertStatements,
          i
      );
    }
  }

  private static String getActualsForErrorMessage(final List<FakeKafkaRecord> actual) {
    final StringBuilder stringBuilder = new StringBuilder("Actual records: \n");
    for (final FakeKafkaRecord fakeKafkaRecord : actual) {
      final ProducerRecord<?, ?> producerRecord = fakeKafkaRecord.getProducerRecord();
      stringBuilder.append(getProducerRecordInString(producerRecord))
          .append(" \n");
    }
    return stringBuilder.toString();
  }

  private static String getProducerRecordInString(final ProducerRecord<?, ?> producerRecord) {
    return "<" + producerRecord.key() + ", "
        + producerRecord.value() + "> with timestamp="
        + producerRecord.timestamp();
  }

  private static void verifyTopology(final TestCase testCase) {
    testCase.getExpectedTopology().ifPresent(expected -> {
      final String expectedTopology = expected.getTopology();
      final String actualTopology = testCase.getGeneratedTopologies().get(0);
      assertThat("Generated topology differs from that built by previous versions of KSQL"
              + " - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          actualTopology, is(expectedTopology));

      expected.getSchemas().ifPresent(schemas -> assertThat("Schemas used by topology differ "
              + "from those used by previous versions"
              + " of KSQL - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          testCase.getGeneratedSchemas().get(0), is(schemas)));
    });
  }

  private static void pipeRecordsFromProvidedInput(
      final TestCase testCase,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer topologyTestDriverContainer,
      final ServiceContext serviceContext
  ) {

    for (final Record record : testCase.getInputRecords()) {
      if (topologyTestDriverContainer.getSourceTopicNames().contains(record.topic.getName())) {
        processSingleRecord(
            testCase,
            FakeKafkaRecord.of(record, null),
            fakeKafkaService,
            topologyTestDriverContainer,
            serviceContext.getSchemaRegistryClient(),
            ImmutableSet.copyOf(fakeKafkaService.getAllTopics())
        );
      }
    }
  }

  private static void pipeRecordsFromKafka(
      final TestCase testCase,
      final String kafkaTopicName,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer topologyTestDriverContainer,
      final ServiceContext serviceContext
  ) {
    for (final FakeKafkaRecord fakeKafkaRecord : fakeKafkaService.readRecords(kafkaTopicName)) {
      processSingleRecord(
          testCase,
          fakeKafkaRecord,
          fakeKafkaService,
          topologyTestDriverContainer,
          serviceContext.getSchemaRegistryClient(),
          Collections.emptySet()
      );
    }
  }

  @SuppressWarnings("unchecked")
  private static void processSingleRecord(
      final TestCase testCase,
      final FakeKafkaRecord inputRecord,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer testDriver,
      final SchemaRegistryClient schemaRegistryClient,
      final Set<Topic> possibleSinkTopics
  ) {
    final Topic recordTopic = fakeKafkaService
        .getTopic(inputRecord.getTestRecord().topic.getName());
    final Serializer<Object> keySerializer = recordTopic.getKeySerializer(schemaRegistryClient);

    final Serializer<Object> valueSerializer =
        recordTopic.getValueSerdeSupplier() instanceof AvroSerdeSupplier
            ? new ValueSpecAvroSerdeSupplier().getSerializer(schemaRegistryClient)
            : recordTopic.getValueSerializer(schemaRegistryClient);

    final Object key = getKey(inputRecord);
    final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecordFactory<>(
        keySerializer,
        valueSerializer
    ).create(
        recordTopic.getName(),
        key,
        inputRecord.getTestRecord().value(),
        inputRecord.getTestRecord().timestamp().orElse(0L)
    );
    testDriver.getTopologyTestDriver().pipeInput(consumerRecord);

    final Topic sinkTopic = testDriver.getSinkTopic();

    processRecordsForTopic(
        testCase,
        testDriver.getTopologyTestDriver(),
        sinkTopic,
        fakeKafkaService,
        schemaRegistryClient
    );

    for (final Topic possibleSinkTopic : possibleSinkTopics) {
      if (possibleSinkTopic.getName().equals(sinkTopic.getName())) {
        continue;
      }
      processRecordsForTopic(
          testCase,
          testDriver.getTopologyTestDriver(),
          possibleSinkTopic,
          fakeKafkaService,
          schemaRegistryClient
      );
    }
  }

  @SuppressWarnings("unchecked")
  private static void processRecordsForTopic(
      final TestCase testCase,
      final TopologyTestDriver topologyTestDriver,
      final Topic sinkTopic,
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final boolean legacySessionWindow = isLegacySessionWindow(testCase.properties());

    while (true) {
      final ProducerRecord<?, ?> producerRecord = topologyTestDriver.readOutput(
          sinkTopic.getName(),
          sinkTopic.getKeyDeserializer(schemaRegistryClient,
              legacySessionWindow),
          sinkTopic.getValueDeserializer(schemaRegistryClient)
      );
      if (producerRecord == null) {
        break;
      }

      fakeKafkaService.writeRecord(
          sinkTopic.getName(),
          FakeKafkaRecord.of(
              sinkTopic,
              producerRecord)
      );
    }
  }

  private static boolean isLegacySessionWindow(final Map<String, Object> properties) {
    final Object config = properties.get(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG);
    return config != null && Boolean.parseBoolean(config.toString());
  }

  private static Object getKey(final FakeKafkaRecord fakeKafkaRecord) {
    return fakeKafkaRecord.getProducerRecord() == null
        ? fakeKafkaRecord.getTestRecord().key()
        : fakeKafkaRecord.getProducerRecord().key();
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

  private static void validateCreatedMessage(
      final String topicName,
      final Record expectedRecord,
      final ProducerRecord<?, ?> actualProducerRecord,
      final boolean ranWithInsertStatements,
      final int messageIndex
  ) {
    final Object actualKey = actualProducerRecord.key();
    final Object actualValue = actualProducerRecord.value();
    final long actualTimestamp = actualProducerRecord.timestamp();

    final Object expectedKey = expectedRecord.key();
    final Object expectedValue = expectedRecord.value();
    final long expectedTimestamp = expectedRecord.timestamp().orElse(actualTimestamp);

    final AssertionError error = new AssertionError(
        "Topic '" + topicName + "', message " + messageIndex
            + ": Expected <" + expectedKey + ", " + expectedValue + "> "
            + "with timestamp=" + expectedTimestamp
            + " but was " + getProducerRecordInString(actualProducerRecord));

    if (!Objects.equals(actualKey, expectedKey)) {
      throw error;
    }

    if (!Objects.equals(actualValue, expectedValue)) {
      throw error;
    }

    if ((actualTimestamp != expectedTimestamp)
        && (!ranWithInsertStatements || expectedTimestamp != 0L)) {
      throw error;
    }
  }

  interface TopologyBuilder {

    List<TopologyTestDriverContainer> buildStreamsTopologyTestDrivers(
        TestCase testCase,
        ServiceContext serviceContext,
        KsqlEngine ksqlEngine,
        KsqlConfig ksqlConfig,
        FakeKafkaService fakeKafkaService
    );
  }
}
