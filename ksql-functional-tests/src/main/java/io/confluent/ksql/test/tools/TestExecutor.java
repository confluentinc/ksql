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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.services.DefaultConnectClient;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.tools.stubs.StubKafkaClientSupplier;
import io.confluent.ksql.test.tools.stubs.StubKafkaRecord;
import io.confluent.ksql.test.tools.stubs.StubKafkaService;
import io.confluent.ksql.test.tools.stubs.StubKafkaTopicClient;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
      .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
      .build();

  private static final Pattern INTERNAL_TOPIC_PATTERN = Pattern
      .compile("_confluent.*query_(.*_\\d+)-.*-(changelog|repartition)");

  private final ServiceContext serviceContext;
  private final KsqlEngine ksqlEngine;
  private final Map<String, ?> config = baseConfig();
  private final StubKafkaService stubKafkaService;
  private final TopologyBuilder topologyBuilder;
  private final Function<TopologyTestDriver, Set<String>> internalTopicsAccessor;

  public TestExecutor() {
    this(StubKafkaService.create(), getServiceContext());
  }

  @VisibleForTesting
  TestExecutor(
      final StubKafkaService stubKafkaService,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final TopologyBuilder topologyBuilder,
      final Function<TopologyTestDriver, Set<String>> internalTopicsAccessor
  ) {
    this.stubKafkaService = requireNonNull(stubKafkaService, "stubKafkaService");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.topologyBuilder = requireNonNull(topologyBuilder, "topologyBuilder");
    this.internalTopicsAccessor = requireNonNull(internalTopicsAccessor, "internalTopicsAccessor");
  }

  private TestExecutor(
      final StubKafkaService stubKafkaService,
      final ServiceContext serviceContext
  ) {
    this(
        stubKafkaService,
        serviceContext,
        getKsqlEngine(serviceContext),
        TestExecutorUtil::buildStreamsTopologyTestDrivers,
        KafkaStreamsInternalTopicsAccessor::getInternalTopics
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
              stubKafkaService
          );

      testCase.expectedException().map(ee -> {
        throw new AssertionError("Expected test to throw" + StringDescription.toString(ee));
      });

      writeInputIntoTopics(testCase.getInputRecords(), stubKafkaService);
      final Set<String> inputTopics = testCase.getInputRecords()
          .stream()
          .map(record -> record.topic.getName())
          .collect(Collectors.toSet());

      final Set<String> allTopicNames = new HashSet<>();

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

        allTopicNames.addAll(
            internalTopicsAccessor.apply(topologyTestDriverContainer.getTopologyTestDriver()));
      }
      verifyOutput(testCase);

      stubKafkaService.getAllTopics().stream()
          .map(Topic::getName)
          .forEach(allTopicNames::add);

      testCase.getPostConditions().verify(ksqlEngine.getMetaStore(), allTopicNames);
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

    final Map<String, List<StubKafkaRecord>> actualByTopic = expectedByTopic.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), stubKafkaService::readRecords));

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
      final List<StubKafkaRecord> actual,
      final boolean ranWithInsertStatements
  ) {
    if (actual.size() != expected.size()) {
      throw new KsqlException("Topic " + topicName + ". Expected <" + expected.size()
          + "> records but it was <" + actual.size() + ">\n" + getActualsForErrorMessage(actual));
    }

    final Function<Object, Object> keyCoercer = keyCoercerForTopic(topicName);

    for (int i = 0; i < expected.size(); i++) {
      final Record expectedRecord;
      try {
        expectedRecord = expected.get(i).coerceKey(keyCoercer);
      } catch (final Exception e) {
        throw new AssertionError(
            "Topic '" + topicName + "', message " + i
                + ": Could not coerce key in test case to required type. " + e.getMessage(), e);
      }
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

  private Function<Object, Object> keyCoercerForTopic(final String topicName) {
    final SqlType keyType = getTopicInfo(topicName)
        .getSchema()
        .key()
        .get(0)
        .type();

    return key -> {
      if (key == null) {
        return null;
      }
      return DefaultSqlValueCoercer.INSTANCE
          .coerce(key, keyType)
          .orElseThrow(() -> new AssertionError("Invalid key value for topic " + topicName + "."
              + System.lineSeparator()
              + "Expected KeyType: " + keyType
              + System.lineSeparator()
              + "Actual KeyType: " + SchemaConverters.javaToSqlConverter().toSqlType(key.getClass())
              + ", key: " + key + "."
              + System.lineSeparator()
              + "This is likely caused by the key type in the test-case not matching the schema."));
    };
  }

  private TopicInfo getTopicInfo(final String topicName) {
    try {
      final java.util.regex.Matcher matcher = INTERNAL_TOPIC_PATTERN.matcher(topicName);
      if (matcher.matches()) {
        // Internal topic:
        final QueryId queryId = new QueryId(matcher.group(1));
        final PersistentQueryMetadata query = ksqlEngine
            .getPersistentQuery(queryId)
            .orElseThrow(() -> new TestFrameworkException("Unknown queryId for internal topic: "
                + queryId));

        final SourceName sinkName = query.getSinkName();
        final DataSource<?> source = ksqlEngine.getMetaStore().getSource(sinkName);
        return new TopicInfo(source.getSchema(), source.getKsqlTopic().getKeyFormat());
      }

      // Source / sink topic:
      final Set<TopicInfo> keyTypes = ksqlEngine.getMetaStore().getAllDataSources().values()
          .stream()
          .filter(source -> source.getKafkaTopicName().equals(topicName))
          .map(source -> new TopicInfo(source.getSchema(), source.getKsqlTopic().getKeyFormat()))
          .collect(Collectors.toSet());

      if (keyTypes.isEmpty()) {
        throw new TestFrameworkException("no source found for topic");
      }

      return Iterables.get(keyTypes, 0);
    } catch (final Exception e) {
      throw new TestFrameworkException("Failed to determine key type for"
          + System.lineSeparator() + "topic: " + topicName
          + System.lineSeparator() + "reason: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Serializer<Object> getKeySerializer(final String topicName) {
    final TopicInfo topicInfo = getTopicInfo(topicName);

    final SerdeSupplier<?> keySerdeSupplier = SerdeUtil
        .getKeySerdeSupplier(topicInfo.getKeyFormat(), topicInfo::getSchema);

    final Serializer<?> serializer = keySerdeSupplier.getSerializer(
        serviceContext.getSchemaRegistryClient()
    );

    serializer.configure(ImmutableMap.of(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
    ), true);

    return (Serializer) serializer;
  }

  private Deserializer<?> getKeyDeserializer(final String topicName) {
    final TopicInfo topicInfo = getTopicInfo(topicName);

    final SerdeSupplier<?> keySerdeSupplier = SerdeUtil
        .getKeySerdeSupplier(topicInfo.getKeyFormat(), topicInfo::getSchema);

    final Deserializer<?> deserializer = keySerdeSupplier.getDeserializer(
        serviceContext.getSchemaRegistryClient()
    );

    deserializer.configure(ImmutableMap.of(), true);

    return deserializer;
  }

  private static String getActualsForErrorMessage(final List<StubKafkaRecord> actual) {
    final StringBuilder stringBuilder = new StringBuilder("Actual records: \n");
    for (final StubKafkaRecord stubKafkaRecord : actual) {
      final ProducerRecord<?, ?> producerRecord = stubKafkaRecord.getProducerRecord();
      stringBuilder.append(getProducerRecordInString(producerRecord))
          .append(" \n");
    }
    return stringBuilder.toString();
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

      expected.getSchemas().ifPresent(schemas -> {
        final List<String> generated = Arrays.asList(
            testCase.getGeneratedSchemas().get(0).split(System.lineSeparator()));
        assertThat("Schemas used by topology differ "
                + "from those used by previous versions"
                + " of KSQL - this is likely to mean there is a non-backwards compatible change.\n"
                + "THIS IS BAD!",
            generated, hasItems(schemas.split(System.lineSeparator())));
      });
    });
  }

  private void pipeRecordsFromProvidedInput(
      final TestCase testCase,
      final TopologyTestDriverContainer topologyTestDriverContainer
  ) {

    for (final Record record : testCase.getInputRecords()) {
      if (topologyTestDriverContainer.getSourceTopicNames().contains(record.topic.getName())) {

        final Record coerced = record.coerceKey(keyCoercerForTopic(record.topic.getName()));

        processSingleRecord(
            StubKafkaRecord.of(coerced, null),
            topologyTestDriverContainer,
            ImmutableSet.copyOf(stubKafkaService.getAllTopics())
        );
      }
    }
  }

  private void pipeRecordsFromKafka(
      final String kafkaTopicName,
      final TopologyTestDriverContainer topologyTestDriverContainer
  ) {
    for (final StubKafkaRecord stubKafkaRecord : stubKafkaService.readRecords(kafkaTopicName)) {
      processSingleRecord(
          stubKafkaRecord,
          topologyTestDriverContainer,
          Collections.emptySet()
      );
    }
  }

  @SuppressWarnings("unchecked")
  private void processSingleRecord(
      final StubKafkaRecord inputRecord,
      final TopologyTestDriverContainer testDriver,
      final Set<Topic> possibleSinkTopics
  ) {
    final Topic recordTopic = stubKafkaService
        .getTopic(inputRecord.getTestRecord().topic.getName());

    final SchemaRegistryClient schemaRegistryClient = serviceContext.getSchemaRegistryClient();

    final Serializer<Object> keySerializer = getKeySerializer(recordTopic.getName());

    final Serializer<Object> valueSerializer =
        recordTopic.getValueSerdeSupplier() instanceof AvroSerdeSupplier
            ? new ValueSpecAvroSerdeSupplier().getSerializer(schemaRegistryClient)
            : recordTopic.getValueSerializer(schemaRegistryClient);

    final Object key = getKey(inputRecord);
    final ConsumerRecord<byte[], byte[]> consumerRecord =
        new org.apache.kafka.streams.test.ConsumerRecordFactory<>(
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
      final ProducerRecord<?, ?> producerRecord = readOutput(topologyTestDriver, sinkTopic, idx);
      if (producerRecord == null) {
        break;
      }

      stubKafkaService.writeRecord(
          sinkTopic.getName(),
          StubKafkaRecord.of(
              sinkTopic,
              producerRecord)
      );
    }
  }

  @SuppressWarnings("unchecked")
  private ProducerRecord<?, ?> readOutput(
      final TopologyTestDriver topologyTestDriver,
      final Topic sinkTopic,
      final int idx
  ) {
    try {
      return topologyTestDriver.readOutput(
          sinkTopic.getName(),
          getKeyDeserializer(sinkTopic.getName()),
          sinkTopic.getValueDeserializer(serviceContext.getSchemaRegistryClient())
      );
    } catch (final Exception e) {
      throw new AssertionError("Topic " + sinkTopic.getName()
          + ": Failed to read record " + idx
          + ". " + e.getMessage(),
          e
      );
    }
  }

  private static Object getKey(final StubKafkaRecord stubKafkaRecord) {
    return stubKafkaRecord.getProducerRecord() == null
        ? stubKafkaRecord.getTestRecord().key()
        : stubKafkaRecord.getProducerRecord().key();
  }

  static ServiceContext getServiceContext() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    return new DefaultServiceContext(
        new StubKafkaClientSupplier(),
        () -> new StubKafkaClientSupplier().getAdmin(Collections.emptyMap()),
        new StubKafkaTopicClient(),
        () -> schemaRegistryClient,
        () -> new DefaultConnectClient("http://localhost:8083", Optional.empty()),
        DisabledKsqlClient::instance
    );
  }

  static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
    final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
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
      final StubKafkaService stubKafkaService
  ) {
    inputRecords.forEach(
        record -> stubKafkaService.writeRecord(
            record.topic.getName(),
            StubKafkaRecord.of(record, null))
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
        StubKafkaService stubKafkaService
    );
  }

  @Immutable
  private static final class TopicInfo {

    private final LogicalSchema schema;
    private final KeyFormat keyFormat;

    public TopicInfo(final LogicalSchema schema, final KeyFormat keyFormat) {
      this.schema = requireNonNull(schema, "schema");
      this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    }

    public KeyFormat getKeyFormat() {
      return keyFormat;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TopicInfo topicInfo = (TopicInfo) o;
      return Objects.equals(schema, topicInfo.schema)
          && Objects.equals(keyFormat, topicInfo.keyFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, keyFormat);
    }
  }
}
