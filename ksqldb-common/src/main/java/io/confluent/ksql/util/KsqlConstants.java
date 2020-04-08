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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;

public final class KsqlConstants {

  private KsqlConstants() {
  }

  public static final String CONFLUENT_AUTHOR = "Confluent";

  public static final String STREAMS_CHANGELOG_TOPIC_SUFFIX = "-changelog";
  public static final String STREAMS_REPARTITION_TOPIC_SUFFIX = "-repartition";

  public static final String SCHEMA_REGISTRY_VALUE_SUFFIX = "-value";

  public static final int legacyDefaultSinkPartitionCount = 4;
  public static final short legacyDefaultSinkReplicaCount = 1;
  public static final long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public static final long defaultCommitIntervalMsConfig = 2000;
  public static final long defaultCacheMaxBytesBufferingConfig = 10000000;
  public static final int defaultNumberOfStreamsThreads = 4;

  public static final String LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT = "ksql.run.script.statements";

  public static final String DOT = ".";
  public static final String ESCAPE = "`";
  public static final String STRUCT_FIELD_REF = "->";

  public static final String AVRO_SCHEMA_NAMESPACE = "io.confluent.ksql.avro_schemas";
  public static final String AVRO_SCHEMA_NAME = "KsqlDataSourceSchema";
  public static final String DEFAULT_AVRO_SCHEMA_FULL_NAME =
      AVRO_SCHEMA_NAMESPACE + "." + AVRO_SCHEMA_NAME;

  public enum KsqlQueryState {
    RUNNING,
    ERROR,
    UNRESPONSIVE,
  }

  public static final Map<KafkaStreams.State, KsqlQueryState> STREAMS_STATE_TO_KSQL_QUERY_STATE;

  static {
    final Map<KafkaStreams.State, KsqlQueryState> stateMapping = new HashMap<>();
    for (KafkaStreams.State state:KafkaStreams.State.values()) {
      stateMapping.put(state, KsqlQueryState.RUNNING);
    }
    stateMapping.put(KafkaStreams.State.ERROR, KsqlQueryState.ERROR);
    STREAMS_STATE_TO_KSQL_QUERY_STATE = ImmutableMap.copyOf(stateMapping);
  }

  /**
   * Default time and date patterns
   */
  public static final String TIME_PATTERN = "HH:mm:ss.SSS";
  public static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'" + TIME_PATTERN;
}
