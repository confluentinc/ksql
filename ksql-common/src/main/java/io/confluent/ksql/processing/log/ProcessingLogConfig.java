/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.processing.log;

import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class ProcessingLogConfig extends AbstractConfig {
  private static final String PROPERTY_PREFIX = "processing.log.";

  private static ProcessingLogConfig INSTANCE = null;

  public static void configure(final Map<?, ?> properties) {
    Objects.requireNonNull(properties);
    if (INSTANCE != null) {
      throw new IllegalStateException("ProcessingLogConfig instance already set");
    }
    INSTANCE = new ProcessingLogConfig(properties);
  }

  public static ProcessingLogConfig getInstance() {
    if (INSTANCE == null) {
      return new ProcessingLogConfig(Collections.emptyMap());
    }
    return INSTANCE;
  }

  private static String propertyName(final String name) {
    return KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + PROPERTY_PREFIX + name;
  }

  public static final String STREAM_NAME = propertyName("stream.name");
  private static final String STREAM_NAME_DEFAULT = "KSQL_PROCESSING_LOG";
  private static final String STREAM_NAME_DOC =
      "If automatic processing log stream creation is enabled, KSQL sets the name of the "
          + "stream to the value of this property.";

  public static final String TOPIC_NAME = propertyName("topic.name");
  public static final String TOPIC_NAME_NOT_SET = "";
  public static final String TOPIC_NAME_DEFAULT_SUFFIX = "ksql_processing_log";
  private static final String TOPIC_NAME_DOC =
      "If automatic processing log topic creation is enabled, KSQL sets the name of the "
          + "topic to the value of this property. If automatic processing log stream "
          + "creation is enabled, KSQL uses this topic to back the stream.";

  public static final String TOPIC_PARTITIONS = propertyName("topic.partitions");
  private static final int TOPIC_PARTITIONS_DEFAULT = 1;
  private static final String TOPIC_PARTITIONS_DOC =
      "If automatic processing log topic creation is enabled, KSQL creates the topic with "
          + "number of partitions set to the value of this property.";

  public static final String TOPIC_REPLICATION_FACTOR = propertyName("topic.replication.factor");
  private static final short TOPIC_REPLICATION_FACTOR_DEFAULT = 1;
  private static final String TOPIC_REPLICATION_FACTOR_DOC =
      "If automatic processing log topic creation is enabled, KSQL creates the topic with "
          + "number of replicas set to the value of this property.";

  public static final String STREAM_AUTO_CREATE = propertyName("stream.auto.create");
  private static final String STREAM_AUTO_CREATE_DOC = String.format(
      "Toggles automatic processing log stream creation. If set to true, and "
          + "running interactive mode on a new cluster, then KSQL will automatically "
          + "create a processing log stream when it starts up. The name for the stream "
          + "is the value of the \"%s\" property. The stream will be created over the topic "
          + "set in the \"%s\" property",
      STREAM_NAME,
      TOPIC_NAME);

  public static final String TOPIC_AUTO_CREATE = propertyName("topic.auto.create");
  private static final String TOPIC_AUTO_CREATE_DOC = String.format(
      "Toggles automatic processing log topic creation. If set to true, then "
          + "KSQL will automatically try to create a processing log topic at startup. "
          + "The name of the topic is the value of the \"%s\" property. The number of "
          + "partitions is taken from the \"%s\" property , and the replication factor "
          + "is taken from the \"%s\" property",
      TOPIC_NAME,
      TOPIC_PARTITIONS,
      TOPIC_REPLICATION_FACTOR);

  public static final String INCLUDE_ROWS = propertyName("include.rows");
  private static final String INCLUDE_ROWS_DOC =
      "Toggles whether or not the processing log should include rows in log messages";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          STREAM_AUTO_CREATE,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          STREAM_AUTO_CREATE_DOC)
      .define(
          STREAM_NAME,
          Type.STRING,
          STREAM_NAME_DEFAULT,
          Importance.MEDIUM,
          STREAM_NAME_DOC)
      .define(
          TOPIC_AUTO_CREATE,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          TOPIC_AUTO_CREATE_DOC)
      .define(
          TOPIC_NAME,
          Type.STRING,
          TOPIC_NAME_NOT_SET,
          Importance.MEDIUM,
          TOPIC_NAME_DOC)
      .define(
          TOPIC_PARTITIONS,
          Type.INT,
          TOPIC_PARTITIONS_DEFAULT,
          Importance.LOW,
          TOPIC_PARTITIONS_DOC)
      .define(
          TOPIC_REPLICATION_FACTOR,
          Type.SHORT,
          TOPIC_REPLICATION_FACTOR_DEFAULT,
          Importance.LOW,
          TOPIC_REPLICATION_FACTOR_DOC)
      .define(
          INCLUDE_ROWS,
          Type.BOOLEAN,
          false,
          Importance.HIGH,
          INCLUDE_ROWS_DOC
      );

  public ProcessingLogConfig(final Map<?, ?> properties) {
    super(CONFIG_DEF, properties);
  }
}
