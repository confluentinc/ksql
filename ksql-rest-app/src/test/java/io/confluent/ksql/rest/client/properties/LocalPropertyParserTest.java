/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.client.properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class LocalPropertyParserTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock(MockType.NICE)
  private PropertiesValidator validator;
  private LocalPropertyParser parser;

  @Before
  public void setUp() {
    parser = new LocalPropertyParser(validator);
  }

  @Test
  public void shouldCallValidatorWithParsedValue() {
    // Given:
    validator.validate(ProducerConfig.LINGER_MS_CONFIG, 100);
    EasyMock.expectLastCall();
    EasyMock.replay(validator);

    // When:
    parser.parse(ProducerConfig.LINGER_MS_CONFIG, "100");

    // Then:
    EasyMock.verify(validator);
  }

  @Test
  public void shouldCallValidatorForValuesThatDoNotNeedParsing() {
    // Given:
    validator.validate(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "10");
    EasyMock.expectLastCall();
    EasyMock.replay(validator);

    // When:
    parser.parse(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "10");

    // Then:
    EasyMock.verify(validator);
  }

  @Test
  public void shouldHandleValueAlreadyBeingTheRightType() {
    assertThat(parser.parse(ProducerConfig.BUFFER_MEMORY_CONFIG, 100L), is(100L));
  }

  @Test
  public void shouldThrowIfValueNotCompatibleType() {
    // Given:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage(containsString("Invalid value"));

    // When:
    parser.parse(ProducerConfig.BUFFER_MEMORY_CONFIG, String.class);
  }

  @Test
  public void shouldThrowIfValueCanNotBeCoerced() {
    // Given:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage(containsString("Invalid value"));

    // When:
    parser.parse(ProducerConfig.BUFFER_MEMORY_CONFIG, "not-a-number");
  }

  @Test
  public void shouldParseStreamsConfig() {
    assertThat(parser.parse(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "100"), is(100));
  }

  @Test
  public void shouldParseConsumerConfig() {
    assertThat(parser.parse(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"), is(100));
  }

  @Test
  public void shouldParseProducerConfig() {
    assertThat(parser.parse(ProducerConfig.BUFFER_MEMORY_CONFIG, "100"), is(100L));
  }

  @Test
  public void shouldParsePrefixedConsumerConfig() {
    assertThat(parser.parse(
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"),
        is(100));
  }

  @Test
  public void shouldParsePrefixedProducerConfig() {
    assertThat(parser.parse(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "100"),
        is(100L));
  }

  @Test
  public void shouldThrowOnUnknownPrefixedConsumerConfig() {
    // Given:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid consumer property: 'unknown'");

    // When:
    parser.parse(StreamsConfig.CONSUMER_PREFIX + "unknown", "100");
  }

  @Test
  public void shouldThrowOnUnknownPrefixedProducerConfig() {
    // Given:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(containsString("Invalid producer property: 'unknown'"));

    // When:
    parser.parse(StreamsConfig.PRODUCER_PREFIX + "unknown", "100");
  }

  @Test
  public void shouldThrowOnUnknownProperty() {
    // Given:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(containsString(
        "Not recognizable as ksql, streams, consumer, or producer property: 'unknown'"));

    // When:
    parser.parse("unknown", "100");
  }
}