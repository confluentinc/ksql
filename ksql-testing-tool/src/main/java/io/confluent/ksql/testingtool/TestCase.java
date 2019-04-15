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

package io.confluent.ksql.testingtool;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.hamcrest.StringDescription;

public class TestCase implements Test {

  private final Path testPath;
  private final String name;
  private final Map<String, Object> properties;
  private final Collection<Topic> topics;
  private final List<Record> inputRecords;
  private final List<Record> outputRecords;
  private final List<String> statements;
  private final ExpectedException expectedException;
  private List<String> generatedTopology;
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

  TestCase(
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

  TestCase copyWithName(final String newName) {
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
    copy.generatedTopology = generatedTopology;
    copy.expectedTopology = expectedTopology;
    copy.generatedSchemas = generatedSchemas;
    return copy;
  }

  void setGeneratedTopology(final List<String> generatedTopology) {
    this.generatedTopology = Objects.requireNonNull(generatedTopology, "generatedTopology");
  }

  void setExpectedTopology(final TopologyAndConfigs expectedTopology) {
    this.expectedTopology = Optional.of(expectedTopology);
  }

  void setGeneratedSchemas(final List<String> generatedSchemas) {
    this.generatedSchemas = Objects.requireNonNull(generatedSchemas, "generatedSchemas");
  }

  Map<String, String> persistedProperties() {
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
  void processInput(final TopologyTestDriverContainer testDriver,
      final SchemaRegistryClient schemaRegistryClient) {
    inputRecords
        .stream().filter(record -> testDriver.getSourceTopics().contains(record.topic.getName()))
        .forEach(
        r -> testDriver.getTopologyTestDriver().pipeInput(
            new ConsumerRecordFactory<>(
                r.keySerializer(),
                r.topic.getSerializer(schemaRegistryClient)
            ).create(r.topic.name, r.key(), r.value, r.timestamp)
        )
    );
  }

  public static void printOutput(final TopologyTestDriverContainer topologyTestDriverContainer,
      final SchemaRegistryClient schemaRegistryClient) {
    for (int i = 0; i < 1; i++) {
      final ProducerRecord record = topologyTestDriverContainer.getTopologyTestDriver().readOutput(
          topologyTestDriverContainer.getSinkTopics().iterator().next(),
          new StringSerdeSupplier().getDeserializer(schemaRegistryClient),
          new StringDeserializer());
      System.out.println(record);
    }
  }

  @SuppressWarnings("unchecked")
  void verifyOutput(final TopologyTestDriverContainer testDriver,
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

  void initializeTopics(final ServiceContext serviceContext) {
    for (final Topic topic : topics) {
      serviceContext.getTopicClient().createTopic(
          topic.getName(),
          topic.numPartitions,
          (short) topic.replicas);

      topic.getSchema()
          .ifPresent(schema -> {
            try {
              serviceContext.getSchemaRegistryClient()
                  .register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  void verifyMetastore(final MetaStore metaStore) {
    postConditions.verify(metaStore);
  }

  void verifyTopology() {
    expectedTopology.ifPresent(expected -> {
      assertThat("Generated topology differs from that built by previous versions of KSQL"
              + " - this likely means there is a non-backwards compatible change.\n"
              + "THIS IS BAD!",
          generatedTopology, is(expected.topology));

      expected.schemas.ifPresent(schemas -> {
        assertThat("Schemas used by topology differ from those used by previous versions"
                + " of KSQL - this likely means there is a non-backwards compatible change.\n"
                + "THIS IS BAD!",
            generatedSchemas, is(schemas));
      });
    });
  }

  boolean isAnyExceptionExpected() {
    return !expectedException.matchers.isEmpty();
  }

  private void failDueToMissingException() {
    final String expectation = StringDescription.toString(expectedException.build());
    final String message = "Expected test to throw" + expectation;
    fail(message);
  }

  void handleException(final RuntimeException e) {
    if (isAnyExceptionExpected()) {
      assertThat(e, isThrowable(expectedException.build()));
    } else {
      throw e;
    }
  }
}