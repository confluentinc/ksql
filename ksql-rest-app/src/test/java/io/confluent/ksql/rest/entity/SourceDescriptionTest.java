/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.math.DoubleMath;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SourceDescriptionTest {
  private final static String CLIENT_ID = "client";
  private final static String APP_ID = "test-app";

  private ConsumerCollector consumerCollector;

  @Before
  public void setUp() {
    consumerCollector = new ConsumerCollector();
    consumerCollector.configure(
        Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID));
  }

  @After
  public void tearDown() {
    StreamsErrorCollector.notifyApplicationClose(APP_ID);
    consumerCollector.close();
  }

  private StructuredDataSource buildDataSource(final String kafkaTopicName) {
    final Schema schema = SchemaBuilder.struct()
        .field("field0", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final KsqlTopic topic = new KsqlTopic("internal", kafkaTopicName, new KsqlJsonTopicSerDe(), true);
    return new KsqlStream<>(
        "query", "stream", schema, schema.fields().get(0),
        new MetadataTimestampExtractionPolicy(), topic, Serdes.String());
  }

  private ConsumerRecords buildRecords(final String kafkaTopicName) {
    return new ConsumerRecords<>(
        ImmutableMap.of(
            new TopicPartition(kafkaTopicName, 1),
            Arrays.asList(
                new ConsumerRecord<>(
                    kafkaTopicName, 1, 1, 1l, TimestampType.CREATE_TIME, 1l,
                    10, 10, "key", "1234567890")
            )
        )
    );
  }

  @Test
  public void shouldReturnStatsBasedOnKafkaTopic() {
    // Given:
    final String kafkaTopicName = "kafka";
    final StructuredDataSource dataSource = buildDataSource(kafkaTopicName);
    consumerCollector.onConsume(buildRecords(kafkaTopicName));
    StreamsErrorCollector.recordError(APP_ID, kafkaTopicName);

    // When
    final SourceDescription sourceDescription = SourceDescription.of(
        dataSource,
        true,
        "json",
        Collections.emptyList(),
        Collections.emptyList(),
        null);

    // Then:
    assertThat(
        sourceDescription.getStatistics(),
        containsString(ConsumerCollector.CONSUMER_TOTAL_MESSAGES));
    assertThat(
        sourceDescription.getErrorStats(),
        containsString(StreamsErrorCollector.CONSUMER_FAILED_MESSAGES));
  }

  @Test
  public void shouldReturnStructuredMetricsBasedOnKafkaTopic() {
    // Given:
    final String kafkaTopicName = "kafka";
    final StructuredDataSource dataSource = buildDataSource(kafkaTopicName);
    consumerCollector.onConsume(buildRecords(kafkaTopicName));
    StreamsErrorCollector.recordError(APP_ID, kafkaTopicName);

    // When
    final SourceDescription sourceDescription = SourceDescription.of(
        dataSource,
        true,
        "json",
        Collections.emptyList(),
        Collections.emptyList(),
        null);

    // Then:
    System.out.println(sourceDescription.getMetrics());
    assertThat(sourceDescription.getMetrics(), matchesMetrics(
        new Metrics(
            ImmutableList.of(
                new Metric(ConsumerCollector.CONSUMER_MESSAGES_PER_SEC, 0d, 0L, false),
                new Metric(ConsumerCollector.CONSUMER_TOTAL_MESSAGES, 1d, 0L, false),
                new Metric(ConsumerCollector.CONSUMER_TOTAL_BYTES, 20d, 0L, false)
            ),
            ImmutableList.of(
                new Metric(StreamsErrorCollector.CONSUMER_FAILED_MESSAGES_PER_SEC, 0d, 0L, false),
                new Metric(StreamsErrorCollector.CONSUMER_FAILED_MESSAGES, 1d, 0L, false)
            )
        )
    ));
  }

  private Matcher<Metrics> matchesMetrics(Metrics expected) {
    return new TypeSafeMatcher<Metrics>() {

      @Override
      protected boolean matchesSafely(final Metrics actual) {
        final Map<String, Metric> expectedStat =
            Maps.uniqueIndex(expected.getMetrics(), Metric::getName);
        final Map<String, Metric> actualStat =
            Maps.uniqueIndex(actual.getMetrics(), Metric::getName);

        final boolean statsMatch = expectedStat.entrySet()
            .stream()
            .map(metric -> matches(metric.getValue(), actualStat.get(metric.getKey())))
            .reduce(Boolean::logicalAnd)
            .orElse(expectedStat.isEmpty() && actualStat.isEmpty());

        final Map<String, Metric> expectedErrors =
            Maps.uniqueIndex(expected.getErrorMetrics(), Metric::getName);
        final Map<String, Metric> actualErrors =
            Maps.uniqueIndex(actual.getErrorMetrics(), Metric::getName);

        final boolean errorsMatch = expectedErrors.entrySet()
            .stream()
            .map(metric -> matches(metric.getValue(), actualErrors.get(metric.getKey())))
            .reduce(Boolean::logicalAnd)
            .orElse(expectedStat.isEmpty() && actualStat.isEmpty());

        return statsMatch && errorsMatch;
      }

      private boolean matches(final Metric actual, final Metric expected) {
        return Objects.equals(expected.getName(), actual.getName())
            && DoubleMath.fuzzyCompare(expected.getValue(), expected.getValue(), .1) == 0;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(expected.format());
      }
    };
  }
}
