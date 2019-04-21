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

package io.confluent.ksql.test.commons;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import io.confluent.ksql.test.commons.services.FakeKafkaService;
import io.confluent.ksql.util.KafkaTopicClient;
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
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.hamcrest.StringDescription;

public class TestCase implements Test {

  private final Path testPath;
  public final String name;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final ExpectedException expectedException;
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
      final ExpectedException expectedException,
      final PostConditions postConditions
  ) {
    this.topics = topics;
    this.inputRecords = inputRecords;
    this.outputRecords = outputRecords;
    this.testPath = testPath;
    this.name = name;
    this.properties = ImmutableMap.copyOf(properties);
    this.statements = statements;
    this.expectedException = expectedException;
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
        expectedException,
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
  public void processInput(final TopologyTestDriverContainer testDriver,
      final SchemaRegistryClient schemaRegistryClient) {
    inputRecords
        .stream().filter(record -> testDriver.getSourceKsqlTopics().contains(record.topic.getName()))
        .forEach(
            r -> testDriver.getTopologyTestDriver().pipeInput(
                new ConsumerRecordFactory<>(
                    r.keySerializer(),
                    r.topic.getSerializer(schemaRegistryClient)
                ).create(r.topic.name, r.key(), r.value, r.timestamp)
            ));
  }

  public static void printOutput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    for (int i = 0; i < 1; i++) {
      final ProducerRecord record = topologyTestDriverContainer.getTopologyTestDriver().readOutput(
          topologyTestDriverContainer.getSinkKsqlTopics().iterator().next().getKafkaTopicName(),
          new StringSerdeSupplier().getDeserializer(schemaRegistryClient),
          new StringDeserializer());
      System.out.println(record);
    }
  }

  public void createInputTopics(final FakeKafkaService fakeKafkaService) {
    topics.forEach(fakeKafkaService::createTopic);
  }

  public void writeInputIntoTopics(
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    inputRecords.forEach(
        record -> fakeKafkaService.writeSingleRecoredIntoTopic(record.topic.getName(), record)
    );
  }

  @SuppressWarnings("unchecked")
  public static void processInputFromTopic(
      final TopologyTestDriverContainer testDriver,
      final FakeKafkaService fakeKafkaService,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final List<Record> inputRecordsFromKafka = new ArrayList<>();
    testDriver.getSourceKsqlTopics().forEach(
        ksqlTopic -> inputRecordsFromKafka.addAll(
            fakeKafkaService.readRecordsFromTopic(ksqlTopic.getKafkaTopicName())
        )
    );
    inputRecordsFromKafka.forEach(
        record -> testDriver.getTopologyTestDriver().pipeInput(
            new ConsumerRecordFactory<>(
                record.keySerializer(),
                record.topic.getSerializer(schemaRegistryClient)
            ).create(record.topic.name, record.key(), record.value, record.timestamp)
        )
    );

    testDriver.getSinkKsqlTopics().forEach(
        ksqlTopic -> {
          try {
            fakeKafkaService.writeSingleRecoredIntoTopic(
                ksqlTopic.getKafkaTopicName(),
                getRecordFromProducerRecord(
                    getTopic(
                        kafkaTopicClient,
                        ksqlTopic,
                        schemaRegistryClient
                    ),
                    testDriver.getTopologyTestDriver().readOutput(
                        ksqlTopic.getKafkaTopicName(),
                        new StringDeserializer(),
                        getSerdeSupplierForKsqlTopic(ksqlTopic)
                            .getDeserializer(schemaRegistryClient))
                )
            );
          } catch (final IOException e) {
            e.printStackTrace();
          } catch (final RestClientException e) {
            e.printStackTrace();
          }
        }
    );
  }

  public void verifyOutputTopics(
      final TopologyTestDriverContainer testDriver,
      final FakeKafkaService fakeKafkaService,
      final SchemaRegistryClient schemaRegistryClient) {
    final Map<String, List<Record>> outputRecordsFromKafka = new HashMap<>();
    testDriver.getSinkKsqlTopics().forEach(
        ksqlTopic -> outputRecordsFromKafka.put(
            ksqlTopic.getKafkaTopicName(),
            fakeKafkaService.readRecordsFromTopic(ksqlTopic.getKafkaTopicName())
        )
    );

    final Map<String, List<Record>> outputRecordsFromTest = getExpectedRecordsMap();

  }

  private Map<String, List<Record>> getExpectedRecordsMap() {
    final Map<String, List<Record>> outputRecordsFromTest = new HashMap<>();
    outputRecords.forEach(
        record ->  { if (!outputRecordsFromTest.containsKey(record.topic.getName())) {
          outputRecordsFromTest.put(record.topic.getName(), new ArrayList<>());
        }
        outputRecordsFromTest.get(record.topic.getName()).add(record);
        }
    );
    return outputRecordsFromTest;
  }

  @SuppressWarnings("unchecked")
  public void verifyOutput(final TopologyTestDriverContainer testDriver,
      final SchemaRegistryClient schemaRegistryClient) {
    if (isAnyExceptionExpected()) {
      failDueToMissingException();
    }

    int idx = -1;
    try {
      for (idx = 0; idx < outputRecords.size(); idx++) {
        final Record expectedOutput = outputRecords.get(idx);

        final ProducerRecord record = testDriver.getTopologyTestDriver().readOutput(
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
      assertThat("Generated topology differs from that built by previous versions of KSQL"
              + " - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          generatedTopologies.get(0), is(expected.topology));

      expected.schemas.ifPresent(schemas -> {
        assertThat("Schemas used by topology differ from those used by previous versions"
                + " of KSQL - this likely means there is a non-backwards compatible change.\n"
                + "THIS IS BAD!",
            generatedSchemas.get(0), is(schemas));
      });
    });
  }

  public boolean isAnyExceptionExpected() {
    return !expectedException.matchers.isEmpty();
  }

  private void failDueToMissingException() {
    final String expectation = StringDescription.toString(expectedException.build());
    final String message = "Expected test to throw" + expectation;
    fail(message);
  }

  public void handleException(final RuntimeException e) {
    if (isAnyExceptionExpected()) {
      assertThat(e, isThrowable(expectedException.build()));
    } else {
      throw e;
    }
  }


  private static Record getRecordFromProducerRecord(
      final Topic topic,
      final ProducerRecord producerRecord) {
    Objects.requireNonNull(producerRecord);
    return new Record(
        topic,
        producerRecord.key().toString(),
        producerRecord.value(),
        producerRecord.timestamp(),
        null
    );
  }

  private static Topic getTopic(
      final KafkaTopicClient kafkaTopicClient,
      final KsqlTopic ksqlTopic,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    Objects.requireNonNull(kafkaTopicClient);
    Objects.requireNonNull(ksqlTopic);
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
    switch (ksqlTopic.getKsqlTopicSerDe().getSerDe()) {
      case JSON:
      case DELIMITED:
        return new StringSerdeSupplier();
      case AVRO:
        return new AvroSerdeSupplier();
      default:
        throw new KsqlException("Unsupported topic serde: "
            + ksqlTopic.getKsqlTopicSerDe().getSerDe());
    }
  }

  private static Optional<Schema> getSchema(
      final KsqlTopic ksqlTopic,
      final SchemaRegistryClient schemaRegistryClient) throws IOException, RestClientException {
    if (ksqlTopic.getKsqlTopicSerDe().getSerDe() != DataSourceSerDe.AVRO) {
      return Optional.empty();
    }
    return Optional.of(new Schema.Parser().parse(
        schemaRegistryClient.getLatestSchemaMetadata(ksqlTopic.getKafkaTopicName()).getSchema()
    ));
  }
}