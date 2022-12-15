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

package io.confluent.ksql.test.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Helper for consuming expected messages from Kafka in integration tests
 */
@SuppressWarnings("WeakerAccess")
public final class ConsumerTestUtil {

  public static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  private ConsumerTestUtil() {
  }

  /**
   * Verify there are {@code expectedCount} messages available to the supplied {@code consumer}.
   *
   * @param consumer the consumer to use.
   * @param expectedCount the number of messages that are expected.
   * @param <V> type of the message, i.e. the Kafka record value type.
   * @return the consumed messages.
   * @throws AssertionError if insufficient messages were available.
   */
  public static <V> List<V> verifyAvailableMessages(
      final Consumer<?, V> consumer,
      final int expectedCount
  ) {
    return verifyAvailableRecords(consumer, expectedCount, DEFAULT_VERIFY_TIMEOUT)
        .stream()
        .map(ConsumerRecord::value)
        .collect(Collectors.toList());
  }

  /**
   * Verify there are {@code expectedCount} records available to the supplied {@code consumer}.
   *
   * @param consumer the consumer to use.
   * @param expectedCount the number of messages that are expected.
   * @param <K> the Kafka record key type.
   * @param <V> the Kafka record value type.
   * @return the consumed records.
   * @throws AssertionError if insufficient records were available.
   */
  public static <K, V> List<ConsumerRecord<K, V>> verifyAvailableRecords(
      final Consumer<K, V> consumer,
      final int expectedCount
  ) {
    return verifyAvailableRecords(consumer, expectedCount, DEFAULT_VERIFY_TIMEOUT);
  }

  /**
   * Verify there are {@code expected} records available to the supplied {@code consumer}.
   *
   * @param consumer the consumer to use.
   * @param expected matcher for the available records.
   * @param <K> the Kafka record key type.
   * @param <V> the Kafka record value type.
   * @return the consumed records.
   * @throws AssertionError if insufficient records were available.
   */
  public static <K, V> List<ConsumerRecord<K, V>> verifyAvailableRecords(
      final Consumer<K, V> consumer,
      final Matcher<? super List<ConsumerRecord<K, V>>> expected
  ) {
    return verifyAvailableRecords(consumer, expected, DEFAULT_VERIFY_TIMEOUT);
  }

  /**
   * Verify there are {@code expectedCount} records available to the supplied {@code consumer}.
   *
   * @param consumer the consumer to use.
   * @param expectedCount the number of messages that are expected.
   * @param timeout how long to wait for the records to be available.
   * @param <K> the Kafka record key type.
   * @param <V> the Kafka record value type.
   * @return the consumed records.
   * @throws AssertionError if insufficient records were available.
   */
  public static <K, V> List<ConsumerRecord<K, V>> verifyAvailableRecords(
      final Consumer<K, V> consumer,
      final int expectedCount,
      final Duration timeout
  ) {
    return verifyAvailableRecords(consumer, hasSize(expectedCount), timeout);
  }

  /**
   * Verify there are {@code expected} records available to the supplied {@code consumer}.
   *
   * @param consumer the consumer to use.
   * @param expected matcher for the available records.
   * @param timeout how long to wait for the records to be available.
   * @param <K> the Kafka record key type.
   * @param <V> the Kafka record value type.
   * @return the consumed records.
   * @throws AssertionError if insufficient records were available.
   */
  public static <K, V> List<ConsumerRecord<K, V>> verifyAvailableRecords(
      final Consumer<K, V> consumer,
      final Matcher<? super List<ConsumerRecord<K, V>>> expected,
      final Duration timeout
  ) {
    final long threshold = System.currentTimeMillis() + timeout.toMillis();

    final List<ConsumerRecord<K, V>> acquired = new ArrayList<>();
    while (System.currentTimeMillis() < threshold && !expected.matches(acquired)) {
      consumer.poll(POLL_TIMEOUT).forEach(acquired::add);
    }

    if (System.currentTimeMillis() < threshold) {
      // One last poll to allow additional records _beyond_ expected to be detected:
      consumer.poll(POLL_TIMEOUT).forEach(acquired::add);
    }

    assertThat("Required records not consumed. Got: "
            + System.lineSeparator()
            + acquired.stream()
            .map(ConsumerRecord::toString)
            .collect(Collectors.joining(System.lineSeparator())),
        acquired, expected);
    return acquired;
  }

  /**
   * Matcher that converts the list of received records into a map, keyed of the message value.
   *
   * @param expected the expected map of unique records.
   * @param <K> the key of the record.
   * @param <V> the value of the record.
   * @return the matcher.
   */
  public static <K, V> Matcher<List<ConsumerRecord<K, V>>> hasUniqueRecords(
      final Matcher<? super Map<K, V>> expected
  ) {
    return new TypeSafeDiagnosingMatcher<List<ConsumerRecord<K, V>>>() {
      @Override
      protected boolean matchesSafely(
          final List<ConsumerRecord<K, V>> actual,
          final Description mismatchDescription
      ) {

        final Map<K, V> uniqueRows = toUniqueRecords(actual);

        if (!expected.matches(uniqueRows)) {
          mismatchDescription.appendText("unique rows ");
          expected.describeMismatch(uniqueRows, mismatchDescription);
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("unique rows ").appendDescriptionOf(expected);
      }
    };
  }

  /**
   * Convert a list of records to a map containing the last value per key.
   *
   * @param records the list of records
   * @param <K> the type of the record key.
   * @param <V> the type of the record value.
   * @return the map of unique records.
   */
  public static <K, V> Map<K, V> toUniqueRecords(
      final List<ConsumerRecord<K, V>> records
  ) {
    // Note: Can't use J8 streams as they can't handle null values:
    final Map<K, V> unique = new HashMap<>(records.size());
    records.forEach(cr -> unique.put(cr.key(), cr.value()));
    return unique;
  }
}
