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

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.services.DefaultConnectClient;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.model.PostConditionsNode.PostTopicNode;
import io.confluent.ksql.test.model.SchemaNode;
import io.confluent.ksql.test.model.SourceNode;
import io.confluent.ksql.test.tools.TopicInfoCache.TopicInfo;
import io.confluent.ksql.test.tools.stubs.StubKafkaClientSupplier;
import io.confluent.ksql.test.tools.stubs.StubKafkaConsumerGroupClient;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.test.tools.stubs.StubKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("deprecation")
public class TestExecutor implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ImmutableMap<String, Object> BASE_CONFIG = ImmutableMap
      .<String, Object>builder()
      .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
      .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
      .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
      .put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0L)
      .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
      .build();

  private final ServiceContext serviceContext;
  private final KsqlEngine ksqlEngine;
  private final Map<String, ?> config = baseConfig();
  private final StubKafkaService kafka;
  private final TopologyBuilder topologyBuilder;
  private final TopicInfoCache topicInfoCache;

  public static TestExecutor create(final Optional<String> extensionDir) {
    final StubKafkaService kafkaService = StubKafkaService.create();
    final StubKafkaClientSupplier kafkaClientSupplier = new StubKafkaClientSupplier();
    final ServiceContext serviceContext = getServiceContext(kafkaClientSupplier);

    return new TestExecutor(
        kafkaService,
        serviceContext,
        getKsqlEngine(serviceContext, extensionDir),
        TestExecutorUtil::buildStreamsTopologyTestDrivers
    );
  }

  @VisibleForTesting
  TestExecutor(
      final StubKafkaService kafka,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final TopologyBuilder topologyBuilder
  ) {
    this.kafka = requireNonNull(kafka, "stubKafkaService");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.topologyBuilder = requireNonNull(topologyBuilder, "topologyBuilder");
    this.topicInfoCache = new TopicInfoCache(ksqlEngine, serviceContext.getSchemaRegistryClient());
  }

  public void buildAndExecuteQuery(
      final TestCase testCase,
      final TestExecutionListener listener
  ) {
    topicInfoCache.clear();

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
              kafka,
              listener
          );

      writeInputIntoTopics(testCase.getInputRecords(), kafka);
      final Set<String> inputTopics = testCase.getInputRecords()
          .stream()
          .map(Record::getTopicName)
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
          pipeRecordsFromProvidedInput(testCase, topologyTestDriverContainer);
        }
        for (final Topic kafkaTopic : topicsFromKafka) {
          pipeRecordsFromKafka(
              kafkaTopic.getName(),
              topologyTestDriverContainer
          );
        }

        topologyTestDriverContainer.getTopologyTestDriver()
            .producedTopicNames()
            .forEach(topicInfoCache::get);
      }

      verifyOutput(testCase);

      testCase.expectedException().map(ee -> {
        throw new AssertionError("Expected test to throw" + StringDescription.toString(ee));
      });

      kafka.getAllTopics().stream()
          .map(Topic::getName)
          .forEach(topicInfoCache::get);

      final List<PostTopicNode> knownTopics = topicInfoCache.all().stream()
          .map(ti -> {
            final Topic topic = kafka.getTopic(ti.getTopicName());

            final OptionalInt partitions = topic == null
                ? OptionalInt.empty()
                : OptionalInt.of(topic.getNumPartitions());

            return new PostTopicNode(
                ti.getTopicName(),
                ti.getKeyFormat(),
                ti.getValueFormat(),
                partitions
            );
          })
          .collect(Collectors.toList());
      final List<SourceNode> knownSources = ksqlEngine.getMetaStore().getAllDataSources().values()
          .stream()
          .map(SourceNode::fromDataSource)
          .collect(Collectors.toList());

      testCase.getPostConditions().verify(ksqlEngine.getMetaStore(), knownTopics);

      listener.runComplete(knownTopics, knownSources);

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
        .collect(Collectors.groupingBy(Record::getTopicName));

    final Map<String, List<ProducerRecord<?, ?>>> actualByTopic = expectedByTopic.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), kafka::readRecords));

    expectedByTopic.forEach((kafkaTopic, expectedRecords) ->
        validateTopicData(
            kafkaTopic,
            expectedRecords,
            actualByTopic.getOrDefault(kafkaTopic, ImmutableList.of()),
            ranWithInsertStatements
        ));
  }

  private void validateTopicData(
      final String topicName,
      final List<Record> expected,
      final Collection<ProducerRecord<?, ?>> actual,
      final boolean ranWithInsertStatements
  ) {
    if (actual.size() != expected.size()) {
      throw new KsqlException("Topic " + topicName + ". Expected <" + expected.size()
          + "> records but it was <" + actual.size() + ">\n" + getActualsForErrorMessage(actual));
    }

    final TopicInfo topicInfo = topicInfoCache.get(topicName);

    final Iterator<Record> expectedIt = expected.iterator();
    final Iterator<ProducerRecord<?, ?>> actualIt = actual.iterator();

    int i = 0;
    while (actualIt.hasNext() && expectedIt.hasNext()) {
      final Record expectedRecord = topicInfo.coerceRecord(expectedIt.next(), i);
      final ProducerRecord<?, ?> actualProducerRecord = actualIt.next();

      validateCreatedMessage(
          topicName,
          expectedRecord,
          actualProducerRecord,
          ranWithInsertStatements,
          i
      );

      i++;
    }
  }

  private static String getActualsForErrorMessage(final Collection<ProducerRecord<?, ?>> actual) {
    return actual.stream()
        .map(TestExecutor::getProducerRecordInString)
        .collect(Collectors.joining(System.lineSeparator(), "Actual records: \n", ""));
  }

  private static String getProducerRecordInString(final ProducerRecord<?, ?> producerRecord) {
    final Object value = producerRecord.value() instanceof String
        ? "\"" + producerRecord.value() + "\""
        : producerRecord.value();

    return "<" + producerRecord.key() + ", "
        + value + "> with timestamp="
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

      final Map<String, SchemaNode> generatedSchemas =
          testCase.getGeneratedSchemas().entrySet().stream()
              .collect(Collectors.toMap(
                  Entry::getKey,
                  e -> SchemaNode.fromPhysicalSchema(e.getValue())));
      for (final Map.Entry<String, SchemaNode> e : expected.getSchemas().entrySet()) {
        assertThat("Schemas used by topology differ "
                + "from those used by previous versions"
                + " of KSQL - this is likely to mean there is a non-backwards compatible change."
                + "\n"
                + "THIS IS BAD!",
            generatedSchemas, hasEntry(e.getKey(), e.getValue()));
      }
      assertThat("Number of schemas generated from topology does not match that from previous "
              + "versions of KSQL - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          generatedSchemas.entrySet(), hasSize(expected.getSchemas().size()));
    });
  }

  private void pipeRecordsFromProvidedInput(
      final TestCase testCase,
      final TopologyTestDriverContainer testDriver
  ) {
    int inputRecordIndex = 0;
    for (final Record record : testCase.getInputRecords()) {
      if (testDriver.getSourceTopicNames().contains(record.getTopicName())) {

        final TopicInfo topicInfo = topicInfoCache.get(record.getTopicName());

        final Record coerced = topicInfo.coerceRecord(record, inputRecordIndex);

        processSingleRecord(
            coerced.asProducerRecord(),
            testDriver,
            ImmutableSet.copyOf(kafka.getAllTopics())
        );
      }
      ++inputRecordIndex;
    }
  }

  private void pipeRecordsFromKafka(
      final String kafkaTopicName,
      final TopologyTestDriverContainer topologyTestDriverContainer
  ) {
    kafka.readRecords(kafkaTopicName)
        .forEach(rec -> processSingleRecord(
            rec,
            topologyTestDriverContainer,
            ImmutableSet.of()
        ));
  }

  private void processSingleRecord(
      final ProducerRecord<?, ?> producedRecord,
      final TopologyTestDriverContainer testDriver,
      final Set<Topic> possibleSinkTopics
  ) {
    final String topicName = producedRecord.topic();
    final TopicInfo topicInfo = topicInfoCache.get(topicName);

    final ConsumerRecord<byte[], byte[]> consumerRecord =
        new org.apache.kafka.streams.test.ConsumerRecordFactory<>(
            topicInfo.getKeySerializer(),
            topicInfo.getValueSerializer()
        ).create(
            topicName,
            producedRecord.key(),
            producedRecord.value(),
            producedRecord.timestamp()
        );

    testDriver.getTopologyTestDriver().pipeInput(consumerRecord);

    final Topic sinkTopic = testDriver.getSinkTopic();

    processRecordsForTopic(
        testDriver.getTopologyTestDriver(),
        sinkTopic
    );

    for (final Topic possibleSinkTopic : possibleSinkTopics) {
      if (possibleSinkTopic.getName().equals(sinkTopic.getName())) {
        continue;
      }
      processRecordsForTopic(
          testDriver.getTopologyTestDriver(),
          possibleSinkTopic
      );
    }
  }

  private void processRecordsForTopic(
      final TopologyTestDriver topologyTestDriver,
      final Topic sinkTopic
  ) {
    int idx = 0;
    while (true) {
      final ProducerRecord<?, ?> producerRecord = readOutput(topologyTestDriver, sinkTopic, idx++);
      if (producerRecord == null) {
        break;
      }

      kafka.writeRecord(producerRecord);
    }
  }

  private ProducerRecord<?, ?> readOutput(
      final TopologyTestDriver topologyTestDriver,
      final Topic sinkTopic,
      final int idx
  ) {
    try {
      final TopicInfo topicInfo = topicInfoCache.get(sinkTopic.getName());

      return topologyTestDriver.readOutput(
          sinkTopic.getName(),
          topicInfo.getKeyDeserializer(),
          topicInfo.getValueDeserializer()
      );
    } catch (final Exception e) {
      throw new AssertionError("Topic " + sinkTopic.getName()
          + ": Failed to read record " + idx
          + ". " + e.getMessage(),
          e
      );
    }
  }

  static ServiceContext getServiceContext() {
    final StubKafkaClientSupplier kafkaClientSupplier = new StubKafkaClientSupplier();
    return getServiceContext(kafkaClientSupplier);
  }

  private static ServiceContext getServiceContext(
      final StubKafkaClientSupplier kafkaClientSupplier
  ) {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    return new DefaultServiceContext(
        kafkaClientSupplier,
        () -> kafkaClientSupplier.getAdmin(Collections.emptyMap()),
        new StubKafkaTopicClient(),
        () -> schemaRegistryClient,
        () -> new DefaultConnectClient("http://localhost:8083", Optional.empty()),
        DisabledKsqlClient::instance,
        new StubKafkaConsumerGroupClient()
    );
  }

  static KsqlEngine getKsqlEngine(final ServiceContext serviceContext,
      final Optional<String> extensionDir) {
    final FunctionRegistry functionRegistry;
    if (extensionDir.isPresent()) {
      final MutableFunctionRegistry mutable = new InternalFunctionRegistry();
      UdfLoaderUtil.load(mutable, extensionDir.get());
      functionRegistry = mutable;
    } else {
      functionRegistry = TestFunctionRegistry.INSTANCE.get();
    }
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        (engine) -> new KsqlEngineMetrics(
            "",
            engine,
            Collections.emptyMap(),
            Optional.empty()),
        new SequentialQueryIdGenerator()
    );
  }

  public static Map<String, ?> baseConfig() {
    return ImmutableMap.<String, Object>builder()
        .putAll(BASE_CONFIG)
        .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        .build();
  }

  private static void writeInputIntoTopics(
      final List<Record> inputRecords,
      final StubKafkaService kafka
  ) {
    inputRecords.stream()
        .map(Record::asProducerRecord)
        .forEach(kafka::writeRecord);
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
    final JsonNode expectedValue = expectedRecord.getJsonValue()
        .orElseThrow(() -> new KsqlServerException(
            "could not get expected value from test record: " + expectedRecord));
    final long expectedTimestamp = expectedRecord.timestamp().orElse(actualTimestamp);

    final AssertionError error = new AssertionError(
        "Topic '" + topicName + "', message " + messageIndex
            + ": Expected <" + expectedKey + ", " + expectedValue + "> "
            + "with timestamp=" + expectedTimestamp
            + " but was " + getProducerRecordInString(actualProducerRecord));

    if (!Objects.equals(actualKey, expectedKey)) {
      throw error;
    }

    if (!ExpectedRecordComparator.matches(actualValue, expectedValue)) {
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
        StubKafkaService stubKafkaService,
        TestExecutionListener listener
    );
  }
}
