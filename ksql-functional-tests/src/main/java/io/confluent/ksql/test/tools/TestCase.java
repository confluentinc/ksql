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
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.test.model.KsqlVersion;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class TestCase implements Test {
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

  @SuppressWarnings("unchecked")
  public void processInput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    final Set<String> sourceKafkaTopicNames = topologyTestDriverContainer.getSourceTopics()
        .stream()
        .map(Topic::getName)
        .collect(Collectors.toSet());
    inputRecords
        .stream().filter(record -> sourceKafkaTopicNames.contains(record.topic.getName()))
        .forEach(
            r -> topologyTestDriverContainer.getTopologyTestDriver().pipeInput(
                new ConsumerRecordFactory<>(
                    r.keySerializer(),
                    r.topic.getSerializer(schemaRegistryClient)
                ).create(r.topic.name, r.key(), r.value, r.timestamp)
            ));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void verifyOutput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    if (isAnyExceptionExpected()) {
      failDueToMissingException();
    }

    int idx = -1;
    try {
      for (idx = 0; idx < outputRecords.size(); idx++) {
        final Record expectedOutput = outputRecords.get(idx);

        final ProducerRecord record = topologyTestDriverContainer.getTopologyTestDriver()
            .readOutput(
                expectedOutput.topic.name,
                expectedOutput.keyDeserializer(),
                expectedOutput.topic.getDeserializer(schemaRegistryClient));

        if (record == null) {
          throw new AssertionError("No record received");
        }

        OutputVerifier.compareKeyValueTimestamp(
            record,
            expectedOutput.key(),
            expectedOutput.value,
            expectedOutput.timestamp);
      }
    } catch (final AssertionError assertionError) {
      final String rowMsg = idx == -1 ? "" : " while processing output row " + idx;
      final String topicMsg = idx == -1 ? "" : " topic: " + outputRecords.get(idx).topic.name;
      throw new AssertionError("TestCase name: "
          + name
          + " in file: " + testPath
          + " failed" + rowMsg + topicMsg + " due to: "
          + assertionError.getMessage(), assertionError);
    }
  }

  public void initializeTopics(
      final KafkaTopicClient kafkaTopicClient,
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient) {
    for (final Topic topic : topics) {
      fakeKafkaService.createTopic(topic);
      kafkaTopicClient.createTopic(
          topic.getName(),
          topic.numPartitions,
          (short) topic.replicas);

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
      final String expectedTopology = standardizeTopology(expected.topology);
      final String actualTopology = standardizeTopology(generatedTopologies.get(0));
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

  private void failDueToMissingException() {
    final String expectation = StringDescription.toString(ksqlExpectedException.build());
    final String message = "Expected test to throw" + expectation;
    fail(message);
  }

  public void handleException(final RuntimeException e) {
    if (isAnyExceptionExpected()) {
      assertThat(e, isThrowable(ksqlExpectedException.build()));
    } else {
      throw e;
    }
  }

  /**
   * Convert a string topology into a standard form.
   *
   * <p>The standardized form takes known compatible changes into account.
   */
  private static String standardizeTopology(final String topology) {
    final String[] lines = topology.split(System.lineSeparator());
    final Pattern aggGroupBy = Pattern.compile("(.*)(--> |<-- |Processor: )Aggregate-groupby(.*)");
    final Pattern linePattern = Pattern.compile("(.*)((?:KSTREAM|KTABLE)-.+-)(\\d+)(.*)");

    final StringBuilder result = new StringBuilder();
    final AtomicInteger nodeCounter = new AtomicInteger();
    final Map<String, String> nodeMappings = new HashMap<>();

    for (String line : lines) {
      final java.util.regex.Matcher aggGroupMatcher = aggGroupBy.matcher(line);
      if (aggGroupMatcher.matches()) {
        line = aggGroupMatcher.group(1)
            + aggGroupMatcher.group(2)
            + "KSTREAM-KEY-SELECT-99999"
            + aggGroupMatcher.group(3);
      }

      final java.util.regex.Matcher mainMatcher = linePattern.matcher(line);
      if (mainMatcher.matches()) {
        final String originalNodeType = mainMatcher.group(2);
        final Integer originalNodeNumber = Integer.valueOf(mainMatcher.group(3));

        final String originalId = originalNodeType + originalNodeNumber;
        final String standardizedId = nodeMappings
            .computeIfAbsent(originalId, key -> originalNodeType + nodeCounter.getAndIncrement());

        line = mainMatcher.group(1) + standardizedId + mainMatcher.group(4);
      }

      result
          .append(line)
          .append(System.lineSeparator());
    }

    return result.toString();
  }

  @SuppressWarnings("unchecked")
  static void processSingleRecord(
      final FakeKafkaRecord fakeKafkaRecord,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer testDriver,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    final Serializer keySerializer = fakeKafkaRecord.getTestRecord().topic.getKeySerializer();
    final Serializer valueSerializer = fakeKafkaRecord.getTestRecord().topic.getSerdeSupplier()
        instanceof AvroSerdeSupplier
        ? new ValueSpecAvroSerdeSupplier().getSerializer(schemaRegistryClient)
        : fakeKafkaRecord.getTestRecord().topic.getSerializer(schemaRegistryClient);
    final Object key = fakeKafkaRecord.getProducerRecord() == null
        ? fakeKafkaRecord.getTestRecord().key()
        : fakeKafkaRecord.getProducerRecord().key();
    final ConsumerRecord consumerRecord = new ConsumerRecordFactory<>(
        keySerializer,
        valueSerializer
    ).create(
        fakeKafkaRecord.getTestRecord().topic.name,
        key,
        fakeKafkaRecord.getTestRecord().value(),
        fakeKafkaRecord.getTestRecord().timestamp
    );
    testDriver.getTopologyTestDriver().pipeInput(consumerRecord);

    final Topic sinkTopic = testDriver.getSinkTopic();

    while (true) {
      final ProducerRecord producerRecord = testDriver.getTopologyTestDriver().readOutput(
          sinkTopic.getName(),
          sinkTopic.getKeyDeserializer(),
          sinkTopic.getDeserializer(schemaRegistryClient)
      );
      if (producerRecord == null) {
        return;
      }

      fakeKafkaService.writeRecord(
          producerRecord.topic(),
          FakeKafkaRecord.of(
              sinkTopic,
              producerRecord)
      );
    }

  }

  void verifyOutputTopics(
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient) {
    final Map<String, List<FakeKafkaRecord>> expectedOutput = getExpectedRecordsMap(
        schemaRegistryClient);
    final Map<String, List<FakeKafkaRecord>> outputRecordsFromKafka = new HashMap<>();
    expectedOutput.keySet().forEach(
        kafkaTopicName -> outputRecordsFromKafka.put(
            kafkaTopicName,
            fakeKafkaService.readRecords(kafkaTopicName)
        )
    );
    expectedOutput.keySet().forEach(kafkaTopic -> validateTopicData(
        expectedOutput.get(kafkaTopic),
        outputRecordsFromKafka.get(kafkaTopic),
        schemaRegistryClient
    ));
  }

  private static void validateTopicData(
      final List<FakeKafkaRecord> expected,
      final List<FakeKafkaRecord> actual,
      final SchemaRegistryClient schemaRegistryClient) {
    if (actual.size() != expected.size()) {
      throw new KsqlException("Expected <" + expected.size()
          + "> records but it was <" + actual.size() + ">");
    }
    for (int i = 0; i < expected.size(); i++) {
      final ProducerRecord actualProducerRecord = actual.get(i).getProducerRecord();
      final ProducerRecord expectedProducerRecord = expected.get(i).getProducerRecord();

      validateCreatedMessage(actualProducerRecord, expectedProducerRecord);
    }
  }

  private static void validateCreatedMessage(
      final ProducerRecord actualProducerRecord,
      final ProducerRecord expectedProducerRecord
  ) {
    final boolean boothValuesNull = (actualProducerRecord.value() == null
        && expectedProducerRecord.value() == null);
    if (
        !actualProducerRecord.timestamp().equals(expectedProducerRecord.timestamp())
            || !actualProducerRecord.key().equals(expectedProducerRecord.key())
            || (!boothValuesNull
            && !actualProducerRecord.value().equals(expectedProducerRecord.value()))) {
      throw new KsqlException(
          "Expected <" + expectedProducerRecord.key() + ", "
              + expectedProducerRecord.value() + "> with timestamp="
              + expectedProducerRecord.timestamp()
              + " but was <" + actualProducerRecord.key() + ", "
              + actualProducerRecord.value() + "> with timestamp="
              + actualProducerRecord.timestamp());
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, List<FakeKafkaRecord>> getExpectedRecordsMap(
      final SchemaRegistryClient schemaRegistryClient) {
    final Map<String, List<FakeKafkaRecord>> outputRecordsFromTest = new HashMap<>();
    outputRecords.forEach(
        record -> {
          if (!outputRecordsFromTest.containsKey(record.topic.getName())) {
            outputRecordsFromTest.put(record.topic.getName(), new ArrayList<>());
          }
          outputRecordsFromTest.get(record.topic.getName()).add(
              FakeKafkaRecord.of(record, new ProducerRecord(
                  record.topic.getName(),
                  null,
                  record.timestamp(),
                  record.key(),
                  record.value()
              )));
        }
    );
    return outputRecordsFromTest;
  }

  private static Optional<Schema> getSchema(
      final KsqlTopic ksqlTopic,
      final SchemaRegistryClient schemaRegistryClient,
      final boolean ignoreSrException) throws IOException, RestClientException {
    if (!(ksqlTopic.getValueSerdeFactory() instanceof KsqlAvroSerdeFactory)) {
      return Optional.empty();
    }
    try {
      return Optional.of(new Schema.Parser().parse(
          schemaRegistryClient.getLatestSchemaMetadata(ksqlTopic.getKafkaTopicName()
              + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX).getSchema()
      ));
    } catch (final IOException | RestClientException e) {
      if (ignoreSrException) {
        return Optional.empty();
      }
      throw e;
    }
  }

}