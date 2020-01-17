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

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class ReservedInternalTopics {
  // This constant should not be part of KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG because it is
  // not configurable. KSQL must always hide its own internal topics.
  public static final String KSQL_INTERNAL_TOPIC_PREFIX = "_confluent-ksql-";

  private final List<Pattern> systemInternalTopics;

  public ReservedInternalTopics(final KsqlConfig ksqlConfig) {
    this.systemInternalTopics = ksqlConfig.getList(KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG)
        .stream()
        .map(Pattern::compile)
        .collect(Collectors.toList());
  }

  public Set<String> filterInternalTopics(final Set<String> topicNames) {
    return topicNames.stream()
        .filter(t -> !isInternalTopic(t))
        .collect(Collectors.toSet());
  }

  public boolean isInternalTopic(final String topicName) {
    return topicName.startsWith(KSQL_INTERNAL_TOPIC_PREFIX) || systemInternalTopics.stream()
        .filter(p -> p.matcher(topicName).matches())
        .findAny()
        .isPresent();
  }
}
