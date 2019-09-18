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

import java.lang.reflect.Field;
import java.util.Set;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Hack to get around the fact that the {@link TopologyTestDriver} class does not expose its set of
 * internal topics, which is needed to determine if any unexpected topics have been created.
 */
final class KafkaStreamsInternalTopicsAccessor {

  private static final Field INTERNAL_TOPICS_FIELD = getInternalTopicsField();

  private KafkaStreamsInternalTopicsAccessor() {
  }

  @SuppressWarnings("unchecked")
  static Set<String> getInternalTopics(
      final TopologyTestDriver topologyTestDriver
  ) {
    try {
      return (Set<String>) INTERNAL_TOPICS_FIELD.get(topologyTestDriver);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Failed to get internal topic names", e);
    }
  }

  private static Field getInternalTopicsField() {
    try {
      final Field field = TopologyTestDriver.class.getDeclaredField("internalTopics");
      field.setAccessible(true);
      return field;
    } catch (final NoSuchFieldException e) {
      throw new AssertionError(
          "Kafka Streams's TopologyTestDriver class has changed its internals", e);
    }
  }
}
