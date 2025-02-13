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

package io.confluent.ksql.util;

import org.apache.kafka.streams.KafkaStreams;

public final class KsqlConstants {

  private KsqlConstants() {
  }

  public static final String CONFLUENT_AUTHOR = "Confluent";

  public static final String STREAMS_CHANGELOG_TOPIC_SUFFIX = "-changelog";
  public static final String STREAMS_REPARTITION_TOPIC_SUFFIX = "-repartition";

  public static final String STREAMS_JOIN_REGISTRATION_TOPIC_PATTERN =
      ".+-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-\\d+-topic";
  public static final String STREAMS_JOIN_RESPONSE_TOPIC_PATTERN =
      ".+-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-\\d+-topic";

  private static final String SCHEMA_REGISTRY_KEY_SUFFIX = "-key";
  private static final String SCHEMA_REGISTRY_VALUE_SUFFIX = "-value";

  public static final long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public static final long defaultCommitIntervalMsConfig = 2000;
  public static final long defaultCacheMaxBytesBufferingConfig = 10000000;
  public static final int defaultNumberOfStreamsThreads = 4;

  public static final String LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT = "ksql.run.script.statements";

  public static final String DOT = ".";
  public static final String STRUCT_FIELD_REF = "->";
  public static final String LAMBDA_FUNCTION = "=>";

  public static final String KSQL_SERVICE_ID_METRICS_TAG = "ksql_service_id";
  public static final String KSQL_QUERY_SOURCE_TAG = "query_source";
  public static final String KSQL_QUERY_PLAN_TYPE_TAG = "query_plan_type";
  public static final String KSQL_QUERY_ROUTING_TYPE_TAG = "query_routing_type";

  public static final String FIPS_VALIDATOR
          = "io.confluent.ksql.security.KsqlFipsResourceExtension";

  public enum KsqlQueryType {
    PERSISTENT,
    PUSH,
    PULL
  }

  public enum PersistentQueryType {
    CREATE_SOURCE,
    CREATE_AS,
    INSERT
  }

  public enum KsqlQueryStatus {
    RUNNING,
    ERROR,
    UNRESPONSIVE,
    PAUSED,
  }

  public static KsqlQueryStatus fromStreamsState(final KafkaStreams.State state) {
    return state == KafkaStreams.State.ERROR ? KsqlQueryStatus.ERROR : KsqlQueryStatus.RUNNING;
  }

  public static String getSRSubject(final String topicName, final boolean isKey) {
    final String suffix = isKey
        ? KsqlConstants.SCHEMA_REGISTRY_KEY_SUFFIX
        : KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
    return topicName + suffix;
  }

  /**
   * Default time and date patterns
   */
  public static final String TIME_PATTERN = "HH:mm:ss.SSS";
  public static final String DATE_PATTERN = "yyyy-MM-dd";
  public static final String DATE_TIME_PATTERN = DATE_PATTERN + "'T'" + TIME_PATTERN;

  /**
   * The types we consider for metrics purposes. These should only be added to. You can deprecate
   * a field, but don't delete it or change its meaning
   */
  public enum QuerySourceType {
    NON_WINDOWED,
    WINDOWED,
    NON_WINDOWED_STREAM,
    WINDOWED_STREAM
  }

  /**
   * The types we consider for metrics purposes. These should only be added to. You can deprecate
   * a field, but don't delete it or change its meaning
   */
  public enum RoutingNodeType {
    SOURCE_NODE,
    REMOTE_NODE
  }
}
