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

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class TestCase implements VersionedTest {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final Path testPath;
  public final String name;
  private final Optional<KsqlVersion> ksqlVersion;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final KsqlExpectedException ksqlExpectedException;
  private List<String> generatedTopologies;
  private List<String> generatedSchemas;
  private Optional<TopologyAndConfigs> expectedTopology = Optional.empty();
  private final PostConditions postConditions;

  @Override
  public String getName() {
    return name;
  }

  List<Record> getInputRecords() {
    return inputRecords;
  }

  public Collection<Topic> getTopics() {
    return topics;
  }

  @Override
  public String getTestFile() {
    return testPath.toString();
  }

  public TestCase(
      final Path testPath,
      final String name,
      final Optional<KsqlVersion> ksqlVersion,
      final Map<String, Object> properties,
      final Collection<Topic> topics,
      final List<Record> inputRecords,
      final List<Record> outputRecords,
      final List<String> statements,
      final KsqlExpectedException ksqlExpectedException,
      final PostConditions postConditions
  ) {
    this.topics = topics;
    this.inputRecords = inputRecords;
    this.outputRecords = outputRecords;
    this.testPath = testPath;
    this.name = name;
    this.ksqlVersion = Objects.requireNonNull(ksqlVersion, "ksqlVersion");
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = statements;
    this.ksqlExpectedException = ksqlExpectedException;
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  public TestCase withVersion(final KsqlVersion version) {
    final String newName = name + "-" + version.getName();
    final TestCase copy = new TestCase(
        testPath,
        newName,
        Optional.of(version),
        properties,
        topics,
        inputRecords,
        outputRecords,
        statements,
        ksqlExpectedException,
        postConditions);
    copy.generatedTopologies = generatedTopologies;
    copy.expectedTopology = expectedTopology;
    copy.generatedSchemas = generatedSchemas;
    return copy;
  }

  public void setGeneratedTopologies(final List<String> generatedTopology) {
    this.generatedTopologies = Objects.requireNonNull(generatedTopology, "generatedTopology");
  }

  public void setExpectedTopology(final TopologyAndConfigs expectedTopology) {
    this.expectedTopology = Optional.of(expectedTopology);
  }

  public void setGeneratedSchemas(final List<String> generatedSchemas) {
    this.generatedSchemas = Objects.requireNonNull(generatedSchemas, "generatedSchemas");
  }

  public Map<String, String> persistedProperties() {
    return expectedTopology
        .flatMap(t -> t.configs)
        .orElseGet(HashMap::new);
  }

  public Map<String, Object> properties() {
    return properties;
  }

  public List<String> statements() {
    return statements;
  }

  public Optional<KsqlVersion> ksqlVersion() {
    return ksqlVersion;
  }

  public Map<Topic, List<Record>> getOutputRecordsByTopic() {
    return outputRecords.stream()
        .collect(Collectors.groupingBy(
            Record::topic
        ));
  }

  @SuppressWarnings("unchecked")
  public void processInput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    final Set<String> sourceKafkaTopicNames = topologyTestDriverContainer.getSourceTopics()
        .stream()
        .map(Topic::getName)
        .collect(Collectors.toSet());
    for (Record record : inputRecords) {
      if (sourceKafkaTopicNames.contains(record.topic.getName())) {
        topologyTestDriverContainer.getTopologyTestDriver().pipeInput(
            new ConsumerRecordFactory<>(
                record.keySerializer(),
                record.topic.getValueSerializer(schemaRegistryClient)
            ).create(
                record.topic.getName(),
                record.key(),
                record.value,
                record.timestamp().orElse(0L))
        );
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void verifyOutput(
      final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    if (isAnyExceptionExpected()) {
      throw failDueToMissingExceptionError();
    }

    int idx = -1;

    try {
      for (idx = 0; idx < outputRecords.size(); idx++) {
        final Record expectedOutput = outputRecords.get(idx);

        final ProducerRecord record = topologyTestDriverContainer.getTopologyTestDriver()
            .readOutput(
                expectedOutput.topic.getName(),
                expectedOutput.keyDeserializer(),
                expectedOutput.topic.getValueDeserializer(schemaRegistryClient));

        if (record == null) {
          throw new AssertionError("No record received");
        }

        OutputVerifier.compareKeyValueTimestamp(
            record,
            expectedOutput.key(),
            expectedOutput.value,
            expectedOutput.timestamp().orElse(record.timestamp()));
      }
    } catch (final AssertionError e) {
      final String rowMsg = idx == -1 ? "" : " while processing output row " + idx;
      final String topicMsg = idx == -1 ? "" : " topic: " + outputRecords.get(idx).topic.getName();
      throw new AssertionError("failed" + rowMsg + topicMsg + " due to: "
          + e.getMessage(), e);
    }

    throwIfMoreOutputAvailable(topologyTestDriverContainer.getTopologyTestDriver());
  }

  private void throwIfMoreOutputAvailable(
      final TopologyTestDriver driver
  ) {
    topics.forEach(topic -> {
      final ProducerRecord<Bytes, Bytes> record = driver.readOutput(
              topic.getName(),
              new BytesDeserializer(),
              new BytesDeserializer());

      if (record != null) {
        throw new AssertionError("Unexpected records available on topic: " + topic.getName());
      }
    });
  }

  public void initializeTopics(
      final KafkaTopicClient kafkaTopicClient,
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient) {
    for (final Topic topic : topics) {
      fakeKafkaService.createTopic(topic);
      kafkaTopicClient.createTopic(
          topic.getName(),
          topic.getNumPartitions(),
          topic.getReplicas());

      topic.getSchema()
          .ifPresent(schema -> {
            try {
              schemaRegistryClient
                  .register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public void verifyMetastore(final MetaStore metaStore) {
    postConditions.verify(metaStore);
  }

  public void verifyTopology() {
    expectedTopology.ifPresent(expected -> {
      final String expectedTopology = expected.topology;
      final String actualTopology = generatedTopologies.get(0);
      assertThat("Generated topology differs from that built by previous versions of KSQL"
              + " - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          actualTopology, is(expectedTopology));

      expected.schemas.ifPresent(schemas -> assertThat("Schemas used by topology differ "
              + "from those used by previous versions"
              + " of KSQL - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          generatedSchemas.get(0), is(schemas)));
    });
  }

  public boolean isAnyExceptionExpected() {
    return !ksqlExpectedException.matchers.isEmpty();
  }

  AssertionError failDueToMissingExceptionError() {
    return new AssertionError("Expected test to throw"
        + StringDescription.toString(ksqlExpectedException.build()));
  }

  public void handleException(final RuntimeException e) {
    if (isAnyExceptionExpected()) {
      assertThat(e, isThrowable(ksqlExpectedException.build()));
    } else {
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  void processSingleRecord(
      final FakeKafkaRecord fakeKafkaRecord,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer testDriver,
      final SchemaRegistryClient schemaRegistryClient,
      final Set<Topic> possibleSinkTopics
  ) {
    final Topic recordTopic = fakeKafkaService
        .getTopic(fakeKafkaRecord.getTestRecord().topic.getName());
    final Serializer<Object> keySerializer = recordTopic.getKeySerializer(schemaRegistryClient);

    final Serializer<Object> valueSerializer =
        recordTopic.getValueSerdeSupplier() instanceof AvroSerdeSupplier
        ? new ValueSpecAvroSerdeSupplier().getSerializer(schemaRegistryClient)
        : recordTopic.getValueSerializer(schemaRegistryClient);

    final Object key = getKey(fakeKafkaRecord);
    final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecordFactory<>(
        keySerializer,
        valueSerializer
    ).create(
        recordTopic.getName(),
        key,
        fakeKafkaRecord.getTestRecord().value(),
        fakeKafkaRecord.getTestRecord().timestamp().orElse(0L)
    );
    testDriver.getTopologyTestDriver().pipeInput(consumerRecord);

    final Topic sinkTopic = testDriver.getSinkTopic();

    processRecordsForTopic(
        testDriver.getTopologyTestDriver(),
        sinkTopic,
        fakeKafkaService,
        schemaRegistryClient
    );

    for (final Topic possibleSinkTopic: possibleSinkTopics) {
      if (possibleSinkTopic.getName().equals(sinkTopic.getName())) {
        continue;
      }
      processRecordsForTopic(
          testDriver.getTopologyTestDriver(),
          possibleSinkTopic,
          fakeKafkaService,
          schemaRegistryClient
      );
    }
  }

  @SuppressWarnings("unchecked")
  private void processRecordsForTopic(
      final TopologyTestDriver topologyTestDriver,
      final Topic sinkTopic,
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    while (true) {
      final ProducerRecord<?,?> producerRecord = topologyTestDriver.readOutput(
          sinkTopic.getName(),
          sinkTopic.getKeyDeserializer(schemaRegistryClient, isLegacySessionWindow()),
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

  void verifyOutputTopics(
      final FakeKafkaService fakeKafkaService
  ) {
    final Map<String, List<Record>> expectedByTopic = outputRecords.stream()
        .collect(Collectors.groupingBy(r -> r.topic().getName()));

    final Map<String, List<FakeKafkaRecord>> actualByTopic = expectedByTopic.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), fakeKafkaService::readRecords));

    expectedByTopic.forEach((kafkaTopic, expectedRecords) ->
        validateTopicData(
            expectedRecords,
            actualByTopic.get(kafkaTopic),
            inputRecords.size() == 0
        ));
  }

  private static void validateTopicData(
      final List<Record> expected,
      final List<FakeKafkaRecord> actual,
      final boolean ranWithInsertStatements) {
    if (actual.size() != expected.size()) {
      throw new KsqlException("Expected <" + expected.size()
          + "> records but it was <" + actual.size() + ">\n" + getActualsForErrorMessage(actual));
    }

    for (int i = 0; i < expected.size(); i++) {
      final Record expectedRecord = expected.get(i);
      final ProducerRecord<?, ?> actualProducerRecord = actual.get(i).getProducerRecord();

      validateCreatedMessage(expectedRecord, actualProducerRecord, ranWithInsertStatements);
    }
  }

  private static String getActualsForErrorMessage(final List<FakeKafkaRecord> actual) {
    final StringBuilder stringBuilder = new StringBuilder("Actual records: \n");
    for (final FakeKafkaRecord fakeKafkaRecord: actual) {
      final ProducerRecord<?,?> producerRecord = fakeKafkaRecord.getProducerRecord();
      stringBuilder.append(getProducerRecordInString(producerRecord))
          .append(" \n");
    }
    return stringBuilder.toString();
  }

  private static void validateCreatedMessage(
      final Record expectedRecord,
      final ProducerRecord<?,?> actualProducerRecord,
      final boolean ranWithInsertStatements
  ) {
    final Object actualKey = actualProducerRecord.key();
    final Object actualValue = actualProducerRecord.value();
    final long actualTimestamp = actualProducerRecord.timestamp();

    final Object expectedKey = expectedRecord.key();
    final Object expectedValue = expectedRecord.value();
    final long expectedTimestamp = expectedRecord.timestamp().orElse(actualTimestamp);

    final AssertionError error = new AssertionError(
        "Expected <" + expectedKey + ", " + expectedValue + "> "
            + "with timestamp=" + expectedTimestamp
            + " but was " + getProducerRecordInString(actualProducerRecord));

    if (actualKey != null) {
      if (!actualKey.equals(expectedKey)) {
        throw error;
      }
    } else if (expectedKey != null) {
      throw error;
    }

    if (actualValue != null) {
      if (!actualValue.equals(expectedValue)) {
        throw error;
      }
    } else if (expectedValue != null) {
      throw error;
    }

    if ((actualTimestamp != expectedTimestamp)
        && (!ranWithInsertStatements || expectedTimestamp != 0L)) {
      throw error;
    }
  }

  private static String getProducerRecordInString(final ProducerRecord<?,?> producerRecord) {
    return "<" + producerRecord.key() + ", "
        + producerRecord.value() + "> with timestamp="
        + producerRecord.timestamp();
  }

  private Map<String, List<FakeKafkaRecord>> getExpectedRecordsMap() {
    final Map<String, List<FakeKafkaRecord>> outputRecordsFromTest = new HashMap<>();
    outputRecords.forEach(
        record -> {
          if (!outputRecordsFromTest.containsKey(record.topic.getName())) {
            outputRecordsFromTest.put(record.topic.getName(), new ArrayList<>());
          }
          outputRecordsFromTest.get(record.topic.getName()).add(
              FakeKafkaRecord.of(record, new ProducerRecord<>(
                  record.topic.getName(),
                  null,
                  record.timestamp().get(),
                  record.key(),
                  record.value()
              )));
        }
    );
    return outputRecordsFromTest;
  }

  private static Object getKey(final FakeKafkaRecord fakeKafkaRecord) {
    return fakeKafkaRecord.getProducerRecord() == null
        ? fakeKafkaRecord.getTestRecord().key()
        : fakeKafkaRecord.getProducerRecord().key();
  }

  private boolean isLegacySessionWindow() {
    final Object config = properties.get(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG);
    return config != null && Boolean.parseBoolean(config.toString());
  }
}