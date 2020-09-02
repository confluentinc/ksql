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

package io.confluent.ksql.properties.with;

import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.serde.Delimiter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;

/**
 * Common 'With Clause' properties
 */
public final class CommonCreateConfigs {

  // Topic Props:
  public static final String KAFKA_TOPIC_NAME_PROPERTY = "KAFKA_TOPIC";
  public static final String SOURCE_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String SOURCE_NUMBER_OF_REPLICAS = "REPLICAS";

  // Timestamp Props:
  public static final String TIMESTAMP_NAME_PROPERTY = "TIMESTAMP";
  public static final String TIMESTAMP_FORMAT_PROPERTY = "TIMESTAMP_FORMAT";

  // Persistence Props:
  public static final String VALUE_AVRO_SCHEMA_FULL_NAME = "VALUE_AVRO_SCHEMA_FULL_NAME";
  public static final String VALUE_FORMAT_PROPERTY = "VALUE_FORMAT";
  public static final String WRAP_SINGLE_VALUE = "WRAP_SINGLE_VALUE";

  public static final String VALUE_DELIMITER_PROPERTY = "VALUE_DELIMITER";

  public static void addToConfigDef(
      final ConfigDef configDef,
      final boolean topicNameRequired,
      final boolean valueFormatRequired
  ) {
    configDef
        .define(
            KAFKA_TOPIC_NAME_PROPERTY,
            ConfigDef.Type.STRING,
            topicNameRequired ? ConfigDef.NO_DEFAULT_VALUE : null,
            new NonEmptyString(),
            Importance.HIGH,
            "The topic that stores the data of the source"
        )
        .define(
            SOURCE_NUMBER_OF_PARTITIONS,
            ConfigDef.Type.INT,
            null,
            Importance.LOW,
            "The number of partitions in the backing topic. This property must be set "
                + "if creating a source without an existing topic (the command will fail if the "
                + "topic does not exist)"
        )
        .define(
            SOURCE_NUMBER_OF_REPLICAS,
            ConfigDef.Type.SHORT,
            null,
            Importance.LOW,
            "The number of replicas in the backing topic. If this property is not set "
                + "but '" + SOURCE_NUMBER_OF_PARTITIONS
                + "' is set, then the default "
                + "Kafka cluster configuration for replicas will be used for creating a new "
                + "topic."
        )
        .define(
            VALUE_FORMAT_PROPERTY,
            ConfigDef.Type.STRING,
            valueFormatRequired ? ConfigDef.NO_DEFAULT_VALUE : null,
            Importance.HIGH,
            "The format of the serialized value"
        )
        .define(
            TIMESTAMP_NAME_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            Importance.MEDIUM,
            "The name of a field within the Kafka record value that contains the "
                + "timestamp KSQL should use inplace of the default Kafka record timestamp. "
                + "By default, KSQL requires the timestamp to be a `BIGINT`. Alternatively, you "
                + "can supply '" + TIMESTAMP_FORMAT_PROPERTY
                + "' to control how the field is parsed"
        )
        .define(
            TIMESTAMP_FORMAT_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            Importance.MEDIUM,
            "If supplied, defines the format of the '"
                + TIMESTAMP_NAME_PROPERTY + "' field. The format can be any supported by "
                + "the Java DateTimeFormatter class"
        )
        .define(
            WRAP_SINGLE_VALUE,
            ConfigDef.Type.BOOLEAN,
            null,
            Importance.LOW,
            "Controls how values are deserialized where the value schema contains "
                + "only a single field.  If set to true, KSQL expects the field to have been "
                + "serialized as a named field within a record. If set to false, KSQL expects the "
                + "field to have been serialized as an anonymous value."
        )
        .define(
            VALUE_AVRO_SCHEMA_FULL_NAME,
            ConfigDef.Type.STRING,
            null,
            Importance.LOW,
            "The fully qualified name of the Avro schema to use"
        )
        .define(
            VALUE_DELIMITER_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            ConfigValidators.nullsAllowed(ConfigValidators.parses(Delimiter::parse)),
            Importance.LOW,
            "The delimiter to use when VALUE_FORMAT='DELIMITED'. Supports single "
              + "character to be a delimiter, defaults to ','. For space and tab delimited values "
              + "you must use the special values 'SPACE' or 'TAB', not an actual space or tab "
              + "character.");
  }

  private CommonCreateConfigs() {
  }
}
