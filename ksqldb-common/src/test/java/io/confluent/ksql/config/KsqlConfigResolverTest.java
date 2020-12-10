/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.config.ConfigItem.Resolved;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.TopicConfig;
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
    assertThat(resolver.resolve(KsqlConfig.CONNECT_URL_PROPERTY, true),
        is(resolvedItem(KsqlConfig.CONNECT_URL_PROPERTY, KSQL_CONFIG_DEF)));
  }

  @Test
  public void shouldNotFindPrefixedKsqlProperty() {
    assertNotFound(
        KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + KsqlConfig.CONNECT_URL_PROPERTY);
  }

  @Test
  public void shouldNotFindUnknownKsqlProperty() {
    assertNotFound(KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + "you.won't.find.me...right");
  }

  @Test
  public void shouldReturnUnresolvedForOtherKsqlFunctionProperty() {
    assertThat(
        resolver.resolve(KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop", true),
        is(unresolvedItem(KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop")));
  }

  @Test
  public void shouldResolveStreamsConfig() {
    assertThat(resolver.resolve(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, true),
        is(resolvedItem(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, STREAMS_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlStreamPrefixedStreamConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, true),
        is(resolvedItem(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, STREAMS_CONFIG_DEF)));
  }

  @Test
  public void shouldReturnUnresolvedForTopicPrefixedStreamsConfig() {
    final String prop = StreamsConfig.TOPIC_PREFIX + TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + prop, true), is(unresolvedItem(prop)));
  }

  @Test
  public void shouldNotFindUnknownStreamsProperty() {
    assertNotFound(KsqlConfig.KSQL_STREAMS_PREFIX + "you.won't.find.me...right");
  }

  @Test
  public void shouldResolveConsumerConfig() {
    assertThat(resolver.resolve(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, true),
        is(resolvedItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveConsumerPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, true),
        is(resolvedItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, true),
        is(resolvedItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlConsumerPrefixedConsumerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, true),
        is(resolvedItem(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_CONFIG_DEF)));
  }

  @Test
  public void shouldNotFindUnknownConsumerPropertyIfStrict() {
    // Given:
    final String configName = StreamsConfig.CONSUMER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, true), is(Optional.empty()));
  }

  @Test
  public void shouldFindUnknownConsumerPropertyIfNotStrict() {
    // Given:
    final String configName = StreamsConfig.CONSUMER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, false), is(unresolvedItem(configName)));
  }

  @Test
  public void shouldNotFindUnknownStreamsPrefixedConsumerPropertyIfStrict() {
    // Given:
    final String configName = KsqlConfig.KSQL_STREAMS_PREFIX
        + StreamsConfig.CONSUMER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, true), is(Optional.empty()));
  }

  @Test
  public void shouldFindUnknownStreamsPrefixedConsumerPropertyIfNotStrict() {
    // Given:
    final String configName = StreamsConfig.CONSUMER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(
        resolver.resolve(KsqlConfig.KSQL_STREAMS_PREFIX + configName, false),
        is(unresolvedItem(configName))
    );
  }

  @Test
  public void shouldResolveProducerConfig() {
    assertThat(resolver.resolve(ProducerConfig.BUFFER_MEMORY_CONFIG, true),
        is(resolvedItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveProducerPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, true),
        is(resolvedItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, true),
        is(resolvedItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldResolveKsqlProducerPrefixedProducerConfig() {
    assertThat(resolver.resolve(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX
            + ProducerConfig.BUFFER_MEMORY_CONFIG, true),
        is(resolvedItem(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_CONFIG_DEF)));
  }

  @Test
  public void shouldNotFindUnknownProducerPropertyIfStrict() {
    // Given:
    final String configName = StreamsConfig.PRODUCER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, true), is(Optional.empty()));
  }

  @Test
  public void shouldFindUnknownProducerPropertyIfNotStrict() {
    // Given:
    final String configName = StreamsConfig.PRODUCER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, false), is(unresolvedItem(configName)));
  }

  @Test
  public void shouldNotFindUnknownStreamsPrefixedProducerPropertyIfStrict() {
    // Given:
    final String configName = KsqlConfig.KSQL_STREAMS_PREFIX
        + StreamsConfig.PRODUCER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(resolver.resolve(configName, true), is(Optional.empty()));
  }

  @Test
  public void shouldFindUnknownStreamsPrefixedProducerPropertyIfNotStrict() {
    // Given:
    final String configName = StreamsConfig.PRODUCER_PREFIX
        + "custom.interceptor.config";

    // Then:
    assertThat(
        resolver.resolve(KsqlConfig.KSQL_STREAMS_PREFIX + configName, false),
        is(unresolvedItem(configName))
    );
  }

  @Test
  public void shouldReturnUnresolvedForOtherConfigIfNotStrict() {
    assertThat(resolver.resolve("confluent.monitoring.interceptor.topic", false),
        is(unresolvedItem("confluent.monitoring.interceptor.topic")));
  }

  @Test
  public void shouldReturnEmptyForOtherConfigIfStrict() {
    assertThat(resolver.resolve("confluent.monitoring.interceptor.topic", true),
        is(Optional.empty()));
  }

  private void assertNotFound(final String configName) {
    assertThat(resolver.resolve(configName, false), is(Optional.empty()));
  }

  private static Matcher<Optional<ConfigItem>> unresolvedItem(final String propertyName) {
    return new TypeSafeDiagnosingMatcher<Optional<ConfigItem>>() {
      @Override
      protected boolean matchesSafely(
          final Optional<ConfigItem> possibleConfig,
          final Description desc) {

        if (!possibleConfig.isPresent()) {
          desc.appendText(" but the name was not known");
          return false;
        }

        final ConfigItem configItem = possibleConfig.get();
        if (!(configItem instanceof ConfigItem.Unresolved)) {
          desc.appendText(" but was resolved item ").appendValue(configItem);
          return false;
        }

        if (!configItem.getPropertyName().equals(propertyName)) {
          desc.appendText(" but propertyName was ").appendValue(configItem.getPropertyName());
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("ConfigItem.Unresolved{propertyName=")
            .appendValue(propertyName)
            .appendText("}");
      }
    };
  }

  private static Matcher<Optional<ConfigItem>> resolvedItem(
      final String propertyName,
      final ConfigDef def) {
    final Optional<ConfigKey> expectedKey = Optional.ofNullable(def)
        .map(d -> d.configKeys().get(propertyName));

    return new TypeSafeDiagnosingMatcher<Optional<ConfigItem>>() {
      @Override
      protected boolean matchesSafely(
          final Optional<ConfigItem> possibleConfig,
          final Description desc) {

        if (!possibleConfig.isPresent()) {
          desc.appendText(" but the name was not known");
          return false;
        }

        final ConfigItem configItem = possibleConfig.get();
        if (!(configItem instanceof ConfigItem.Resolved)) {
          desc.appendText(" but was unresolved item ").appendValue(configItem);
          return false;
        }

        if (!configItem.getPropertyName().equals(propertyName)) {
          desc.appendText(" but propertyName was ").appendValue(configItem.getPropertyName());
          return false;
        }

        final ConfigItem.Resolved resolvedItem = (Resolved) configItem;
        if (expectedKey.map(k -> !k.equals(resolvedItem.getKey())).orElse(false)) {
          desc.appendText(" but key was ").appendValue(resolvedItem.getKey());
          return false;
        }

        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description
            .appendText("ConfigItem.Resolved{key=")
            .appendValue(expectedKey)
            .appendText("}");
      }
    };
  }
}