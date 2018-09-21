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

package io.confluent.ksql.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;


public class KsqlConfigResolverTest {

  private static final ConfigDef STREAMS_CONFIG_DEF = StreamsConfig.configDef();
  private static final ConfigDef CONSUMER_CONFIG_DEF = KsqlConfigResolver
      .getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = KsqlConfigResolver
      .getConfigDef(ProducerConfig.class);
  private static final ConfigDef KSQL_CONFIG_DEF = KsqlConfig.CURRENT_DEF;

  private io.confluent.ksql.config.KsqlConfigResolver resolver;

  @Before
  public void setUp() {
    resolver = new KsqlConfigResolver();
  }

  @Test
  public void shouldResolveKsqlProperty() {
    assertThat(resolver.resolve(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY),
        is(configItem(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, KSQL_CONFIG_DEF)));
  }

  @Test
  public void shouldNotResolvePrefixedKsqlProperty() {
    assertNotResolvable(
        KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
  }

  @Test
  public void shouldNotResolveUnknownKsqlProperty() {
    assertNotResolvable(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + "you.won't.find.me...right");
  }

  @Test
  public void shouldResolveStreamsConfig() {
    assertThat(resolver.resolve(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG),
        is(configItem(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, STREAMS_CONFIG_DEF)));
  }

  @Test
  public void shouldNotResolveStreamPrefixedStreamConfig() {
    assertNotResolvable("streams." + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
  }

  @Test
  public void shouldResolveKsqlStreamPrefixedStreamConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG),
        is(configItem(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, STREAMS_CONFIG_DEF)));
  }

  @Test
  public void shouldNotResolveUnknownStreamsProperty() {
    assertNotResolvable(KsqlConfig.KSQL_STREAMS_PREFIX + "you.won't.find.me...right");
  }

  @Test
  public void shouldResolveConsumerConfig() {
    assertThat(resolver.resolve(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(configItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveConsumerPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(configItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(configItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlConsumerPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(configItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldNotResolveConsumerConfigWithAnyOtherPrefix() {
    assertNotResolvable("what the?" + ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
  }

  @Test
  public void shouldNotResolveUnknownConsumerProperty() {
    assertNotResolvable(StreamsConfig.CONSUMER_PREFIX + "you.won't.find.me...right");
  }

  @Test
  public void shouldResolveProducerConfig() {
    assertThat(resolver.resolve(ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(configItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveProducerPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(configItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(configItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlProducerPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX
            + ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(configItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldNotResolveProducerConfigWithAnyOtherPrefix() {
    assertNotResolvable("what the?" + ProducerConfig.BUFFER_MEMORY_CONFIG);
  }

  @Test
  public void shouldNotResolveUnknownProducerProperty() {
    assertNotResolvable(StreamsConfig.PRODUCER_PREFIX + "you.won't.find.me...right");
  }

  private void assertNotResolvable(final String configName) {
    assertThat(resolver.resolve(configName), is(Optional.empty()));
  }

  private static Matcher<Optional<ConfigItem>> configItem(
      final String propertyName,
      final ConfigDef def) {
    return new TypeSafeDiagnosingMatcher<Optional<ConfigItem>>() {
      @Override
      protected boolean matchesSafely(
          final Optional<ConfigItem> possibleConfig,
          final Description desc) {

        if (!possibleConfig.isPresent()) {
          desc.appendText(" but the name did not resolve");
          return false;
        }

        final ConfigItem configItem = possibleConfig.get();
        if (!configItem.getPropertyName().equals(propertyName)) {
          desc.appendText(" but propertyName was ").appendValue(configItem.getPropertyName());
          return false;
        }

        if (!isEqual(configItem.getDef(), def)) {
          desc.appendText(" but def was ").appendValue(getDefName(configItem.getDef()));
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("ConfigItem{propertyName=").appendValue(propertyName)
            .appendText(", def=").appendValue(getDefName(def));
      }
    };
  }

  private static String getDefName(final ConfigDef def) {
    if (isEqual(def, PRODUCER_CONFIG_DEF)) {
      return "PRODUCER_CONFIG_DEF";
    }

    if (isEqual(def, CONSUMER_CONFIG_DEF)) {
      return "CONSUMER_CONFIG_DEF";
    }

    if (isEqual(def, KSQL_CONFIG_DEF)) {
      return "KSQL_CONFIG_DEF";
    }

    if (isEqual(def, STREAMS_CONFIG_DEF)) {
      return "STREAMS_CONFIG_DEF";
    }

    return "UNKNOWN_DEF";
  }

  private static boolean isEqual(final ConfigDef lhs, final ConfigDef rhs) {
    return lhs.configKeys().equals(rhs.configKeys());
  }
}