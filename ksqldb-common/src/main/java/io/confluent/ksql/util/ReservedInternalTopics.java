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

import com.google.common.collect.Streams;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ReservedInternalTopics {
  private static final Logger LOG = LogManager.getLogger(ReservedInternalTopics.class);

  // These constant should not be part of KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG because they're
  // not configurable.
  public static final String CONFLUENT_PREFIX = "_confluent-";
  public static final String KSQL_INTERNAL_TOPIC_PREFIX = CONFLUENT_PREFIX + "ksql-";
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
   * Returns the KSQL processing log topic.
   * <p/>
   * This is not an internal topic in the sense that users are intentionally meant to read from
   * this topic to identify deserialization and other processing errors, define a KSQL stream on
   * it, and potentially issue queries to filter from it, etc. This is why it is not prefixed in
   * the way KSQL internal topics are.
   *
   * @param config The Processing log config, which is used to extract the processing topic suffix
   * @param ksqlConfig The KSQL config, which is used to extract the KSQL service id.
   * @return The processing log topic name.
   */
  public static String processingLogTopic(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig
  ) {
    final String topicNameConfig = config.getString(ProcessingLogConfig.TOPIC_NAME);
    if (topicNameConfig.equals(ProcessingLogConfig.TOPIC_NAME_NOT_SET)) {
      return String.format(
          "%s%s",
          ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
          ProcessingLogConfig.TOPIC_NAME_DEFAULT_SUFFIX
      );
    } else {
      return topicNameConfig;
    }
  }

  /**
   * Compute a name for a KSQL internal topic.
   *
   * @param ksqlConfig The KSQL config, which is used to extract the internal topic prefix.
   * @param topicSuffix A suffix that is appended to the topic name.
   * @return The computed topic name.
   */
  private static String toKsqlInternalTopic(final KsqlConfig ksqlConfig, final String topicSuffix) {
    return String.format(
        "%s%s_%s",
        KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        topicSuffix
    );
  }

  private final Pattern hiddenTopicsPattern;
  private final Pattern readOnlyTopicsPattern;

  public ReservedInternalTopics(final KsqlConfig ksqlConfig) {
    final ProcessingLogConfig processingLogConfig =
        new ProcessingLogConfig(ksqlConfig.getProcessingLogConfigProps());

    this.hiddenTopicsPattern = Pattern.compile(
        Streams.concat(
            Stream.of(KSQL_INTERNAL_TOPIC_PREFIX + ".*"),
            ksqlConfig.getList(KsqlConfig.KSQL_HIDDEN_TOPICS_CONFIG).stream()
        ).collect(Collectors.joining("|"))
    );

    this.readOnlyTopicsPattern = Pattern.compile(
        Streams.concat(
            Stream.of(processingLogTopic(processingLogConfig, ksqlConfig)),
            Stream.of(KSQL_INTERNAL_TOPIC_PREFIX + ".*"),
            ksqlConfig.getList(KsqlConfig.KSQL_READONLY_TOPICS_CONFIG).stream()
        ).collect(Collectors.joining("|"))
    );
  }

  public Set<String> removeHiddenTopics(final Set<String> topicNames) {
    return topicNames.stream()
        .filter(t -> !isHidden(t))
        .collect(Collectors.toSet());
  }

  public boolean isHidden(final String topicName) {
    return hiddenTopicsPattern.matcher(topicName).matches();
  }

  public boolean isReadOnly(final String topicName) {
    return readOnlyTopicsPattern.matcher(topicName).matches();
  }
}
