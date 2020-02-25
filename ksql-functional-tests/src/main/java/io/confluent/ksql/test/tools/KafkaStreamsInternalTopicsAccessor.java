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
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Hack to get around the fact that the {@link TopologyTestDriver} class does not expose its set of
 * internal topics, which is needed to determine if any unexpected topics have been created.
 *
 * <p>Note: We should find a better way of doing this - this approach is very brittle
 */
final class KafkaStreamsInternalTopicsAccessor {

  private static final Field INTERNAL_TOPOLOGY_BUILDER_FIELD = getInternalTopologyBuilderField();
  private static final Field INTERNAL_TOPIC_NAMES_FIELD = getInternalTopicNamesField();

  private KafkaStreamsInternalTopicsAccessor() {
  }

  @SuppressWarnings("unchecked")
  static Set<String> getInternalTopics(
      final TopologyTestDriver topologyTestDriver
  ) {
    try {
      final Object internalTopologyBuilder = INTERNAL_TOPOLOGY_BUILDER_FIELD
          .get(topologyTestDriver);
      // Note - there is no memory barrier here so we could end up reading stale data if
      // the internal topics are updated
      return new HashSet<>((Set<String>) INTERNAL_TOPIC_NAMES_FIELD.get(internalTopologyBuilder));
    } catch (final IllegalAccessException e) {
      throw new AssertionError("Failed to get internal topic names", e);
    }
  }

  private static Field getInternalTopologyBuilderField() {
    return getField("internalTopologyBuilder", TopologyTestDriver.class);
  }

  private static Field getInternalTopicNamesField() {
    return getField("internalTopicNames", InternalTopologyBuilder.class);
  }

  private static Field getField(final String fieldName, final Class<?> clazz) {
    try {
      final Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field;
    } catch (final NoSuchFieldException e) {
      throw new AssertionError(
          "Kafka Streams's has changed its internals", e);
    }
  }
}
