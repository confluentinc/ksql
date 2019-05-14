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
import io.confluent.ksql.test.serde.ValueSpec;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.avro.ValueSpecAvroSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.TopologyTestDriverContainer.WindowType;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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

  @SuppressWarnings("unchecked")
  static void processSingleRecord(
      final FakeKafkaRecord fakeKafkaRecord,
      final FakeKafkaService fakeKafkaService,
      final TopologyTestDriverContainer testDriver,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    final Serializer serializer = fakeKafkaRecord.getTestRecord().topic.getSerdeSupplier()
        instanceof AvroSerdeSupplier
        ? new ValueSpecAvroSerdeSupplier().getSerializer(schemaRegistryClient)
        : fakeKafkaRecord.getTestRecord().topic.getSerializer(schemaRegistryClient);
    testDriver.getTopologyTestDriver().pipeInput(
        new ConsumerRecordFactory<>(
            fakeKafkaRecord.getTestRecord().keySerializer(),
            serializer
        ).create(
            fakeKafkaRecord.getTestRecord().topic.name,
            fakeKafkaRecord.getTestRecord().key(),
            fakeKafkaRecord.getTestRecord().value(),
            fakeKafkaRecord.getTestRecord().timestamp
        )
    );

    final Topic sinkTopic = getTopic(
        kafkaTopicClient,
        testDriver.getSinkKsqlTopic(),
        schemaRegistryClient,
        true);
    final Deserializer sinkValueDeserializer = sinkTopic.getSerdeSupplier()
        instanceof AvroSerdeSupplier
        ? new ValueSpecAvroSerdeSupplier().getDeserializer(schemaRegistryClient)
        : sinkTopic.getDeserializer(schemaRegistryClient);
    final Deserializer keyDeserializer;
    boolean unnestRedundantWindow = false;
    if (testDriver.getWindow().getLeft() == WindowType.NO_WINDOW) {
      keyDeserializer = Serdes.String().deserializer();
    } else {
      final Deserializer<String> inner = Serdes.String().deserializer();
      if (testDriver.getWindow().getLeft() == WindowType.SESSION) {
        keyDeserializer = new SessionWindowedDeserializer<>(inner);
      } else {
        if (testDriver.getWindow().getRight() != Long.MAX_VALUE) {
          keyDeserializer = new TimeWindowedDeserializer<>(
              inner,
              testDriver.getWindow().getRight());
        } else {
          keyDeserializer = fakeKafkaRecord.getTestRecord().keyDeserializer();
          unnestRedundantWindow = true;
        }
      }
    }
    ProducerRecord producerRecord = testDriver.getTopologyTestDriver().readOutput(
        testDriver.getSinkKsqlTopic().getKafkaTopicName(),
        keyDeserializer,
        sinkValueDeserializer
    );
    if (unnestRedundantWindow) {
      producerRecord = new ProducerRecord(
          producerRecord.topic(),
          producerRecord.partition(),
          producerRecord.timestamp(),
          getWindowed(
              ((Windowed) producerRecord.key()).key().toString(), testDriver.getWindow().getLeft()),
          producerRecord.value()
      );
    }
    if (producerRecord != null) {
      fakeKafkaService.writeRecord(
          producerRecord.topic(),
          FakeKafkaRecord.of(
              sinkTopic,
              producerRecord,
              testDriver.getWindow().getLeft())
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
    assertThat(expected.size(), equalTo(actual.size()));
    for (int i = 0; i < expected.size(); i++) {
      final ProducerRecord actualProducerRecord = actual.get(i).getProducerRecord();
      final ProducerRecord expectedProducerRecord = expected.get(i).getProducerRecord();
      final Object value;
      if (expectedProducerRecord.value() == null) {
        value = null;
      } else if (expected.get(i).getTestRecord().topic.getSerdeSupplier()
          instanceof ValueSpecAvroSerdeSupplier) {
        final ValueSpecAvroSerdeSupplier valueSpecAvroSerdeSupplier
            = new ValueSpecAvroSerdeSupplier();
        final Deserializer deserializer = valueSpecAvroSerdeSupplier
            .getDeserializer(schemaRegistryClient);
        value = ((ValueSpec) deserializer.deserialize(
            expectedProducerRecord.topic(),
            (byte[]) expectedProducerRecord.value())).getSpec();
      } else {
        value = new String((byte[]) expectedProducerRecord.value(), Charset.forName("UTF-8"));
      }

      validateCreatedMessage(actualProducerRecord, expectedProducerRecord, value);
    }
  }

  private static void validateCreatedMessage(
      final ProducerRecord actualProducerRecord,
      final ProducerRecord expectedProducerRecord,
      final Object value
  ) {
    final boolean boothValuesNull = (actualProducerRecord.value() == null && value == null);
    if (
        !actualProducerRecord.timestamp().equals(expectedProducerRecord.timestamp())
            || !actualProducerRecord.key().equals(expectedProducerRecord.key())
            || (!boothValuesNull && !actualProducerRecord.value().equals(value))) {
      throw new KsqlException(
          "Expected <" + expectedProducerRecord.key() + ", "
              + value + "> with timestamp="
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
      final SchemaRegistryClient schemaRegistryClient,
      final boolean ignoreSrException) throws IOException, RestClientException {
    final TopicDescription topicDescription = kafkaTopicClient
        .describeTopic(ksqlTopic.getKafkaTopicName());
    return new Topic(
        ksqlTopic.getKafkaTopicName(),
        getSchema(ksqlTopic, schemaRegistryClient, ignoreSrException),
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

  @SuppressWarnings("unchecked")
  private static Windowed getWindowed(final String windowString, final WindowType windowType) {
    final int atIndex = windowString.indexOf("@");
    final int slashIndex = windowString.indexOf("/");
    final String key = windowString.substring(1, atIndex);
    final Long start = Long.parseLong(windowString.substring(atIndex + 1, slashIndex));
    final Long end = Long.parseLong(windowString.substring(
        slashIndex + 1,
        windowString.length() - 1));
    if (windowType == WindowType.SESSION) {
      return new Windowed(key, new SessionWindow(start, end));
    }
    return new Windowed(key, new TimeWindow(start, end));
  }
}