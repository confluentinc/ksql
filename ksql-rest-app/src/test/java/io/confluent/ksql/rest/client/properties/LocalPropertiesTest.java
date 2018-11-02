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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.PropertyParser;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class LocalPropertiesTest {

  private static final Map<String, Object> INITIAL = ImmutableMap.of(
      "prop-1", "initial-val-1",
      "prop-2", "initial-val-2"
  );

  @Mock(MockType.NICE)
  private PropertyParser parser;
  private LocalProperties propsWithMockParser;
  private LocalProperties realProps;

  @Before
  public void setUp() {
    propsWithMockParser = new LocalProperties(INITIAL, parser);

    expect(parser.parse(anyString(), anyObject()))
        .andReturn("parsed-val");

    replay(parser);
    realProps = new LocalProperties(ImmutableMap.of());
  }

  @Test
  public void shouldGetInitialValues() {
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetInitialValue() {
    // When:
    final Object oldValue = propsWithMockParser.unset("prop-1");

    // Then:
    assertThat(oldValue, is("initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldOverrideInitialValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("prop-1", "new-val");

    // Then:
    assertThat(oldValue, is("initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("parsed-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldOverrideInitialValueWithNewUnparsedValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("prop-1", "new-val", false);

    // Then:
    assertThat(oldValue, is("initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("new-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetOverriddenValue() {
    // Given:
    propsWithMockParser.set("prop-1", "new-val");

    // When:
    final Object oldValue = propsWithMockParser.unset("prop-1");

    // Then:
    assertThat(oldValue, is("parsed-val"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldReturnOnlyKnownKeys() {
    assertThat(propsWithMockParser.toMap().keySet(), containsInAnyOrder("prop-1", "prop-2"));
  }

  @Test
  public void shouldSetNewValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("new-prop", "new-val");

    // Then:
    assertThat(oldValue, is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("new-prop"), is("parsed-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldSetNewUnparsedValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("new-prop", "new-val", false);

    // Then:
    assertThat(oldValue, is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("new-prop"), is("new-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldUnsetNewValue() {
    // Given:
    propsWithMockParser.set("new-prop", "new-val");

    // When:
    final Object oldValue = propsWithMockParser.unset("new-prop");

    // Then:
    assertThat(oldValue, is("parsed-val"));
    assertThat(propsWithMockParser.toMap().get("new-prop"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("initial-val-2"));
  }

  @Test
  public void shouldInvokeParserCorrectly() {
    // Given:
    reset(parser);
    expect(parser.parse("prop-1", "new-val")).andReturn("parsed-new-val");
    replay(parser);

    // When:
    propsWithMockParser.set("prop-1", "new-val");

    // Then:
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("parsed-new-val"));
    verify(parser);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfParserThrows() {
    // Given:
    reset(parser);
    expect(parser.parse("prop-1", "new-val"))
        .andThrow(new IllegalArgumentException("Boom"));
    replay(parser);

    // When:
    propsWithMockParser.set("prop-1", "new-val");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownPropertyToBeSet() {
    realProps.set("some.unknown.prop", "some.value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownConsumerPropertyToBeSet() {
    realProps.set(StreamsConfig.CONSUMER_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test
  public void shouldAllowKnownConsumerPropertyToBeSet() {
    realProps.set(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100");
  }

  @Test
  public void shouldAllowKnownPrefixedConsumerPropertyToBeSet() {
    realProps.set(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100");
  }

  @Test
  public void shouldAllowKnownKsqlPrefixedConsumerPropertyToBeSet() {
    realProps.set(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
        + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownProducerPropertyToBeSet() {
    realProps.set(StreamsConfig.PRODUCER_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test
  public void shouldAllowKnownProducerPropertyToBeSet() {
    realProps.set(ProducerConfig.BUFFER_MEMORY_CONFIG, "100");
  }

  @Test
  public void shouldAllowKnownKsqlPrefixedProducerPropertyToBeSet() {
    realProps.set(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX
        + ProducerConfig.BUFFER_MEMORY_CONFIG, "100");
  }

  @Test
  public void shouldAllowKnownPrefixedProducerPropertyToBeSet() {
    realProps.set(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "100");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownStreamsConfigToBeSet() {
    realProps.set(KsqlConfig.KSQL_STREAMS_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test
  public void shouldAllowKnownStreamsConfigToBeSet() {
    realProps.set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
  }

  @Test
  public void shouldAllowKnownPrefixedStreamsConfigToBeSet() {
    realProps.set(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownKsqlConfigToBeSet() {
    realProps.set(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test
  public void shouldAllowKnownUdfConfigToBeSet() {
    realProps.set(KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG, "true");
  }

  @Test
  public void shouldAllowUnknownUdfConfigToBeSet() {
    realProps.set(KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop", "some thing");
  }
}