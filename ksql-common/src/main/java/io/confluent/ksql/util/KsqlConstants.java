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

  public static final String SINK_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String SINK_NUMBER_OF_REPLICAS = "REPLICAS";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION";
  public static final String SINK_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";

  public static int defaultSinkNumberOfPartitions = 4;
  public static short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public static long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public static String defaultAutoOffsetRestConfig = "latest";
  public static long defaultCommitIntervalMsConfig = 2000;
  public static long defaultCacheMaxBytesBufferingConfig = 10000000;
  public static int defaultNumberOfStreamsThreads = 4;

}
