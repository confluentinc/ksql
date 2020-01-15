/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public final class ReservedInternalTopics {
  private static final Set<String> ALL_INTERNAL_TOPICS_PREFIXES = ImmutableSet.of(
      // Confluent
      "_confluent",
      "__confluent"
  );

  private static final Set<String> ALL_INTERNAL_TOPICS_LITERALS = ImmutableSet.of(
      // Security
      "_secrets",

      // Kafka
      "__consumer_offsets",
      "__transaction_state",

      // Replicator
      "__consumer_timestamps",

      // Schema Registry
      "_schemas",

      // Connect
      "connect-configs",
      "connect-offsets",
      "connect-status",
      "connect-statuses"
  );

  public static boolean isInternalTopic(final String topicName) {
    return isLiteral(topicName) || isPrefix(topicName);
  }

  private static boolean isPrefix(final String topicName) {
    return ALL_INTERNAL_TOPICS_PREFIXES.stream()
        .filter(topicName::startsWith)
        .findAny()
        .isPresent();
  }

  private static boolean isLiteral(final String topicName) {
    return ALL_INTERNAL_TOPICS_LITERALS.contains(topicName);
  }

  private ReservedInternalTopics() {
  }
}
