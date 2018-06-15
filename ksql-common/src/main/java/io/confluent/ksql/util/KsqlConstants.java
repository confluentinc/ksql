/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

public class KsqlConstants {

  public static final String KSQL_INTERNAL_TOPIC_PREFIX = "_confluent-ksql-";
  public static final String CONFLUENT_INTERNAL_TOPIC_PREFIX = "__confluent";

  public static final String SINK_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String SINK_NUMBER_OF_REPLICAS = "REPLICAS";

  public static final String SINK_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";

  public static final String STREAMS_CHANGELOG_TOPIC_SUFFIX = "-changelog";
  public static final String STREAMS_REPARTITION_TOPIC_SUFFIX = "-repartition";

  public static final String SCHEMA_REGISTRY_VALUE_SUFFIX = "-value";
  public static final String AVRO_SCHEMA_ID = "AVRO_SCHEMA_ID";

  public static final int defaultSinkNumberOfPartitions = 4;
  public static final short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public static final long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public static final String defaultAutoOffsetRestConfig = "latest";
  public static final long defaultCommitIntervalMsConfig = 2000;
  public static final long defaultCacheMaxBytesBufferingConfig = 10000000;
  public static final int defaultNumberOfStreamsThreads = 4;

  public static final String RUN_SCRIPT_STATEMENTS_CONTENT = "ksql.run.script.statements";
}
