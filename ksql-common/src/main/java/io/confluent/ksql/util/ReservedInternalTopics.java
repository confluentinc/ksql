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
  // These constant should not be part of KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG because they're
  // not configurable.
  public static final String KSQL_INTERNAL_TOPIC_PREFIX = "_confluent-ksql-";
  public static final String KSQL_COMMAND_TOPIC_SUFFIX = "command_topic";
  public static final String KSQL_CONFIGS_TOPIC_SUFFIX = "configs";

  /**
   * Returns the internal KSQL command topic.
   *
   * @param ksqlConfig The KSQL config, which is used to extract the internal topic prefix.
   * @return The command topic name.
   */
  public static String commandTopic(final KsqlConfig ksqlConfig) {
    return toKsqlInternalTopic(ksqlConfig, KSQL_COMMAND_TOPIC_SUFFIX);
  }

  /**
   * Returns the internal KSQL configs topic (used for KSQL standalone)
   *
   * @param ksqlConfig The KSQL config, which is used to extract the internal topic prefix.
   * @return The configurations topic name.
   */
  public static String configsTopic(final KsqlConfig ksqlConfig) {
    return toKsqlInternalTopic(ksqlConfig, KSQL_CONFIGS_TOPIC_SUFFIX);
  }

  /**
   * Compute a name for a KSQL internal topic.
   *
   * @param ksqlConfig The KSQL config, which is used to extract the internal topic prefix.
   * @param topicSuffix A suffix that is appended to the topic name.
   * @return The computed topic name.
   */
  public static String toKsqlInternalTopic(final KsqlConfig ksqlConfig, final String topicSuffix) {
    return String.format(
        "%s%s_%s",
        KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        topicSuffix
    );
  }

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
