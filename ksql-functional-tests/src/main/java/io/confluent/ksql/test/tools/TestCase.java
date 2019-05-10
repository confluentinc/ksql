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

import static org.hamcrest.Matchers.equalTo;
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
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.json.ValueSpecJsonSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.nio.charset.Charset;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.hamcrest.StringDescription;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class TestCase implements Test {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final Path testPath;
  public final String name;
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

  @Override
  public String getTestFile() {
    return testPath.toString();
  }

  public TestCase(
      final Path testPath,
      final String name,
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
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = statements;
    this.ksqlExpectedException = ksqlExpectedException;
    this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
  }

  public TestCase copyWithName(final String newName) {
    final TestCase copy = new TestCase(
        testPath,
        newName,
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

  @SuppressWarnings("unchecked")
  public void processInput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    final Set<String> sourceKafkaTopicNames = topologyTestDriverContainer.getSourceKsqlTopics()
        .stream()
        .map(KsqlTopic::getKafkaTopicName)
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
      final SchemaRegistryClient schemaRegistryClient) {
    for (final Topic topic : topics) {
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

  public void createInputTopics(final FakeKafkaService fakeKafkaService) {
    topics.forEach(fakeKafkaService::createTopic);
  }

  public void writeInputIntoTopics(
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    inputRecords.forEach(
        record -> fakeKafkaService.writeRecord(
            record.topic.getName(),
            FakeKafkaRecord.of(record, null))
    );
  }

  @SuppressWarnings("unchecked")
  public static void processInputFromTopic(
      final TopologyTestDriverContainer testDriver,
      final FakeKafkaService fakeKafkaService,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    Objects.requireNonNull(testDriver, "TopologyTestDriverContainer");
    Objects.requireNonNull(fakeKafkaService, "fakeKafkaService");
    Objects.requireNonNull(kafkaTopicClient, "kafkaTopicClient");
    Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient");
    for (final KsqlTopic ksqlTopic : testDriver.getSourceKsqlTopics()) {
      for (final FakeKafkaRecord fakeKafkaRecord: fakeKafkaService
          .readRecords(ksqlTopic.getKafkaTopicName())) {
        try {
          processSingleRecord(
              fakeKafkaRecord,
              fakeKafkaService,
              testDriver,
              kafkaTopicClient,
              schemaRegistryClient);
        } catch (IOException | RestClientException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void processSingleRecord(
      final FakeKafkaRecord fakeKafkaRecord,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer testDriver,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    testDriver.getTopologyTestDriver().pipeInput(
        new ConsumerRecordFactory<>(
            fakeKafkaRecord.getTestRecord().keySerializer(),
            fakeKafkaRecord.getTestRecord().topic.getSerializer(schemaRegistryClient)
        ).create(
            fakeKafkaRecord.getTestRecord().topic.name,
            fakeKafkaRecord.getTestRecord().key(),
            fakeKafkaRecord.getTestRecord().value,
            fakeKafkaRecord.getTestRecord().timestamp
        )
    );
    final Topic sinkTopic = getTopic(
        kafkaTopicClient,
        testDriver.getSinkKsqlTopic(),
        schemaRegistryClient);
    final ProducerRecord producerRecord = testDriver.getTopologyTestDriver().readOutput(
        testDriver.getSinkKsqlTopic().getKafkaTopicName(),
        new StringDeserializer(),
        sinkTopic.getDeserializer(schemaRegistryClient)
    );
    if (producerRecord != null) {
      fakeKafkaService.writeRecord(
          producerRecord.topic(),
          FakeKafkaRecord.of(
              getTopic(
                  kafkaTopicClient,
                  testDriver.getSinkKsqlTopic(),
                  schemaRegistryClient),
              producerRecord)
      );
    }
  }

  public void verifyOutputTopics(
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
    assertThat(expected.size(), equalTo(actual.size()));
    for (int i = 0; i < expected.size(); i++) {
      final ProducerRecord actualProducerRecord = actual.get(i).getProducerRecord();
      final ProducerRecord expectedProducerRecord = expected.get(i).getProducerRecord();
      final Object value;
      if (expected.get(i).getTestRecord().topic.getSerdeSupplier()
          instanceof ValueSpecJsonSerdeSupplier) {
        value = new String((byte[]) expectedProducerRecord.value(), Charset.forName("UTF-8"));
      } else {
        final Deserializer deserializer = expected.get(i).getTestRecord().topic
            .getDeserializer(schemaRegistryClient);
        value = deserializer.deserialize("", (byte[]) expectedProducerRecord.value());
      }

      if (
          !actualProducerRecord.timestamp().equals(expectedProducerRecord.timestamp())
          || !actualProducerRecord.key().equals(expectedProducerRecord.key())
          || !actualProducerRecord.value().equals(value)
          ) {
        final AssertionError error = new AssertionError(
            "Expected <" + expectedProducerRecord.key() + ", "
                + value + "> with timestamp="
                + expectedProducerRecord.timestamp()
                + " but was <" + actualProducerRecord.key() + ", "
                + actualProducerRecord.value() + "> with timestamp="
                + actualProducerRecord.timestamp());
        throw new KsqlException(error.getMessage());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, List<FakeKafkaRecord>> getExpectedRecordsMap(
      final SchemaRegistryClient schemaRegistryClient) {
    final Map<String, List<FakeKafkaRecord>> outputRecordsFromTest = new HashMap<>();
    outputRecords.forEach(
        record ->  {
          if (!outputRecordsFromTest.containsKey(record.topic.getName())) {
            outputRecordsFromTest.put(record.topic.getName(), new ArrayList<>());
          }
          outputRecordsFromTest.get(record.topic.getName()).add(
              FakeKafkaRecord.of(record, new ProducerRecord(
                  record.topic.getName(),
                  null,
                  record.timestamp(),
                  record.key(),
                  record.topic.getSerializer(schemaRegistryClient).serialize(
                      record.topic.getName(),
                      record.value()
                  )
              )));
        }
    );
    return outputRecordsFromTest;
  }

  private static Topic getTopic(
      final KafkaTopicClient kafkaTopicClient,
      final KsqlTopic ksqlTopic,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    final TopicDescription topicDescription = kafkaTopicClient
        .describeTopic(ksqlTopic.getKafkaTopicName());
    return new Topic(
        ksqlTopic.getKafkaTopicName(),
        getSchema(ksqlTopic, schemaRegistryClient),
        getSerdeSupplierForKsqlTopic(ksqlTopic),
        topicDescription.partitions().size(),
        topicDescription.partitions().get(0).replicas().size()
    );
  }

  private static SerdeSupplier getSerdeSupplierForKsqlTopic(final KsqlTopic ksqlTopic) {
    Objects.requireNonNull(ksqlTopic);
    if (ksqlTopic.getValueSerdeFactory() instanceof KsqlJsonSerdeFactory
        || ksqlTopic.getValueSerdeFactory() instanceof KsqlDelimitedSerdeFactory) {
      return new StringSerdeSupplier();
    } else if (ksqlTopic.getValueSerdeFactory() instanceof KsqlAvroSerdeFactory) {
      return new AvroSerdeSupplier();
    }
    throw new KsqlException("Unsupported topic serde: "
        + ksqlTopic.getValueSerdeFactory().getClass().getSimpleName());
  }

  private static Optional<Schema> getSchema(
      final KsqlTopic ksqlTopic,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    if (!(ksqlTopic.getValueSerdeFactory() instanceof KsqlAvroSerdeFactory)) {
      return Optional.empty();
    }
    return Optional.of(new Schema.Parser().parse(
        schemaRegistryClient.getLatestSchemaMetadata(ksqlTopic.getKafkaTopicName()).getSchema()
    ));
  }
}