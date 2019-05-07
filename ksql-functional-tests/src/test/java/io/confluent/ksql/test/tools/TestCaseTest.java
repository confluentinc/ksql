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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.serde.avro.AvroSerdeSupplier;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.KsqlExpectedException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestCaseTest {

  @Rule
  public final org.junit.rules.ExpectedException expectedException = org.junit.rules.ExpectedException
      .none();

  private final SerdeSupplier serdeSupplier = new StringSerdeSupplier();
  private final Topic topic = new Topic("foo_kafka", Optional.empty(), serdeSupplier, 4, 1);
  private final Record record = new Record(topic, "k1", "v1, v2", 123456789L, null);
  private final TestCase testCase = new TestCase(
      null,
      "test",
      Collections.emptyMap(),
      ImmutableList.of(topic),
      ImmutableList.of(record),
      ImmutableList.of(record),
      Collections.emptyList(),
      KsqlExpectedException.none(),
      PostConditions.NONE
  );


  @Mock
  private TopologyTestDriver topologyTestDriver;
  @Captor
  private ArgumentCaptor<ConsumerRecord> captor;


  @Before
  public void init() {

  }


  @Test
  @SuppressWarnings("unchecked")
  public void shouldProcessInputRecords() {
    // Given:
    final TopologyTestDriverContainer topologyTestDriverContainer = getSampleTopologyTestDriverContainer();


    // When:
    testCase.processInput(topologyTestDriverContainer, null);


    // Then:
    verify(topologyTestDriver).pipeInput(captor.capture());
    assertThat(captor.getValue().topic(), equalTo(record.topic.getName()));
    assertThat(new String((byte[])captor.getValue().key(), StandardCharsets.UTF_8), equalTo("k1"));
    assertThat(new String((byte[])captor.getValue().value(), StandardCharsets.UTF_8), equalTo("v1, v2"));
    assertThat(captor.getValue().timestamp(), equalTo(record.timestamp));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFilterNonSourceTopics() {
    // Given:
    final TopologyTestDriverContainer topologyTestDriverContainer = TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableSet.of(new KsqlTopic("FOO", "foo_kafka_different_input", new KsqlJsonTopicSerDe(), false)),
        ImmutableSet.of(new KsqlTopic("BAR", "bar_kafka", new KsqlJsonTopicSerDe(), false))
    );


    // When:
    testCase.processInput(topologyTestDriverContainer, null);

    // Then:
    verify(topologyTestDriver, never()).pipeInput(any(ConsumerRecord.class));

  }

  @Test
  public void shouldValidateOutputCorrectly() {
    // Given:
    final TopologyTestDriverContainer topologyTestDriverContainer = getSampleTopologyTestDriverContainer();
    when(topologyTestDriver.readOutput(any(), any(), any()))
        .thenReturn(new ProducerRecord<>("bar_kafka", 1, 123456789L, "k1", "v1, v2"));


    // When:
    testCase.verifyOutput(topologyTestDriverContainer, null);

  }

  @Test
  public void shouldFailForIncorrectOutput() {
    // Given:
    final TopologyTestDriverContainer topologyTestDriverContainer = getSampleTopologyTestDriverContainer();
    when(topologyTestDriver.readOutput(any(), any(), any()))
        .thenReturn(new ProducerRecord<>("bar_kafka", 1, 123456789L, "k12", "v1, v2"));

    // Expect
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage("TestCase name: test in file: null failed while processing output row 0 topic: "
        + "foo_kafka due to: Expected <k1, v1, v2> with timestamp=123456789 "
        + "but was <k12, v1, v2> with timestamp=123456789");


    // When:
    testCase.verifyOutput(topologyTestDriverContainer, null);

  }

  @Test
  public void shouldCreateTopicInInitialization() {
    // Given:
    final KafkaTopicClient kafkaTopicClient = mock(KafkaTopicClient.class);

    // When:
    testCase.initializeTopics(kafkaTopicClient, null);

    // Then:
    verify(kafkaTopicClient).createTopic("foo_kafka", 4, (short)1);
  }

  @Test
  public void shouldRegisterSchemaInInitialization() throws IOException, RestClientException {
    // Given:
    final KafkaTopicClient kafkaTopicClient = mock(KafkaTopicClient.class);
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final Schema fakeAvroSchema = mock(Schema.class);
    final Topic topic = new Topic("foo", Optional.of(fakeAvroSchema), new AvroSerdeSupplier(), 4, (short)1);
    final TestCase testCase = new TestCase(
        null,
        "test",
        Collections.emptyMap(),
        ImmutableList.of(topic),
        ImmutableList.of(record),
        ImmutableList.of(record),
        Collections.emptyList(),
        KsqlExpectedException.none(),
        PostConditions.NONE
    );

    // When:
    testCase.initializeTopics(kafkaTopicClient, schemaRegistryClient);

    // Then:
    verify(schemaRegistryClient).register("foo-value", fakeAvroSchema);

  }

  @Test
  public void shouldVerifyTopology() {
    // Given:
    testCase.setGeneratedTopologies(ImmutableList.of("Test_topology"));
    final TopologyAndConfigs topologyAndConfigs = new TopologyAndConfigs(
        "Test_topology", Optional.empty(), Optional.empty());
    testCase.setExpectedTopology(topologyAndConfigs);

    // When:
    testCase.verifyTopology();
  }

  @Test
  public void shouldFailForInvalidToplogy() {
    // Given:
    testCase.setGeneratedTopologies(ImmutableList.of("Test_topology"));
    final TopologyAndConfigs topologyAndConfigs = new TopologyAndConfigs(
        "Test_topology1", Optional.empty(), Optional.empty());
    testCase.setExpectedTopology(topologyAndConfigs);
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage("Generated topology differs from that built by previous versions of KSQL - this likely means there is a non-backwards compatible change.\n"
        + "THIS IS BAD!\n"
        + "Expected: is \"Test_topology1\\n\"\n"
        + "     but: was \"Test_topology\\n\"");

    // When:
    testCase.verifyTopology();
  }

  private TopologyTestDriverContainer getSampleTopologyTestDriverContainer() {
    return TopologyTestDriverContainer.of(
        topologyTestDriver,
        ImmutableSet.of(new KsqlTopic("FOO", "foo_kafka", new KsqlJsonTopicSerDe(), false)),
        ImmutableSet.of(new KsqlTopic("BAR", "bar_kafka", new KsqlJsonTopicSerDe(), false))
    );
  }

}