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
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
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
  public static final String SOURCE_TOPIC_RETENTION_IN_MS = "RETENTION_MS";
  public static final String SOURCE_TOPIC_CLEANUP_POLICY = "CLEANUP_POLICY";

  // Timestamp Props:
  public static final String TIMESTAMP_NAME_PROPERTY = "TIMESTAMP";
  public static final String TIMESTAMP_FORMAT_PROPERTY = "TIMESTAMP_FORMAT";

  // Persistence Props:
  public static final String VALUE_AVRO_SCHEMA_FULL_NAME = "VALUE_AVRO_SCHEMA_FULL_NAME";
  public static final String KEY_SCHEMA_FULL_NAME = "KEY_SCHEMA_FULL_NAME";
  public static final String VALUE_SCHEMA_FULL_NAME = "VALUE_SCHEMA_FULL_NAME";
  public static final String VALUE_FORMAT_PROPERTY = "VALUE_FORMAT";
  public static final String KEY_FORMAT_PROPERTY = "KEY_FORMAT";
  public static final String FORMAT_PROPERTY = "FORMAT";
  public static final String WRAP_SINGLE_VALUE = "WRAP_SINGLE_VALUE";
  public static final String KEY_PROTOBUF_NULLABLE_REPRESENTATION =
      "KEY_PROTOBUF_NULLABLE_REPRESENTATION";
  public static final String VALUE_PROTOBUF_NULLABLE_REPRESENTATION =
      "VALUE_PROTOBUF_NULLABLE_REPRESENTATION";

  public enum ProtobufNullableConfigValues {
    OPTIONAL,
    WRAPPER
  }

  public static final String VALUE_DELIMITER_PROPERTY = "VALUE_DELIMITER";
  public static final String KEY_DELIMITER_PROPERTY = "KEY_DELIMITER";

  // Schema Props:
  public static final String KEY_SCHEMA_ID = "KEY_SCHEMA_ID";
  public static final String VALUE_SCHEMA_ID = "VALUE_SCHEMA_ID";

  @SuppressWarnings("checkstyle:MethodLength")
  public static void addToConfigDef(
      final ConfigDef configDef,
      final boolean topicNameRequired
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
            SOURCE_TOPIC_RETENTION_IN_MS,
            ConfigDef.Type.LONG,
            null,
            Importance.MEDIUM,
            "The retention in milliseconds in the backing topic. If this property is"
                + "not set then the default value of 7 days will be used for creating a new topic."
        )
        .define(
            VALUE_FORMAT_PROPERTY,
            ConfigDef.Type.STRING,
            null,
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
                + "By default, KSQL requires the timestamp to be a `BIGINT` or a `TIMESTAMP`. "
                + "Alternatively, you can supply '" + TIMESTAMP_FORMAT_PROPERTY + "' to control "
                + "how the field is parsed"
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
            KEY_PROTOBUF_NULLABLE_REPRESENTATION,
            ConfigDef.Type.STRING,
            null,
            ConfigValidators.enumValues(ProtobufNullableConfigValues.class),
            Importance.LOW,
            "If supplied, protobuf schema generation will use fields that distinguish "
                + "null from default values for primitive values. The value `"
                + ProtobufNullableConfigValues.OPTIONAL.name()
                + "` will enable using the `optional` on all fields, whereas `"
                + ProtobufNullableConfigValues.WRAPPER.name()
                + "` will use wrappers for all primitive value fields, including strings."
        )
        .define(
            VALUE_PROTOBUF_NULLABLE_REPRESENTATION,
            ConfigDef.Type.STRING,
            null,
            ConfigValidators.enumValues(ProtobufNullableConfigValues.class),
            Importance.LOW,
            "If supplied, protobuf schema generation will use fields that distinguish "
                + "null from default values for primitive values. The value `"
                + ProtobufNullableConfigValues.OPTIONAL.name()
                + "` will enable using the `optional` on all fields, whereas `"
                + ProtobufNullableConfigValues.WRAPPER.name()
                + "` will use wrappers for all primitive value fields, including strings."
        )
        .define(
            VALUE_AVRO_SCHEMA_FULL_NAME,
            ConfigDef.Type.STRING,
            null,
            Importance.LOW,
            "The fully qualified name of the Avro schema to use for value"
        )
        .define(
            KEY_SCHEMA_FULL_NAME,
            ConfigDef.Type.STRING,
            null,
            Importance.LOW,
            "The fully qualified name of the schema to use"
        )
        .define(
            VALUE_SCHEMA_FULL_NAME,
            ConfigDef.Type.STRING,
            null,
            Importance.LOW,
            "The fully qualified name of the schema to use"
        )
        .define(
            KEY_DELIMITER_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            ConfigValidators.nullsAllowed(ConfigValidators.parses(Delimiter::parse)),
            Importance.LOW,
            "The delimiter to use when KEY_FORMAT='DELIMITED'. Supports single "
                + "character to be a delimiter, defaults to ','. For space and tab delimited "
                + "values you must use the special values 'SPACE' or 'TAB', not an actual space "
                + "or tab character. Also see " + VALUE_DELIMITER_PROPERTY)
        .define(
            VALUE_DELIMITER_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            ConfigValidators.nullsAllowed(ConfigValidators.parses(Delimiter::parse)),
            Importance.LOW,
            "The delimiter to use when VALUE_FORMAT='DELIMITED'. Supports single "
                + "character to be a delimiter, defaults to ','. For space and tab delimited "
                + "values you must use the special values 'SPACE' or 'TAB', not an actual space "
                + "or tab character. Also see " + KEY_DELIMITER_PROPERTY)
        .define(
            KEY_FORMAT_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            Importance.HIGH,
            "The format of the serialized key"
        )
        .define(
            FORMAT_PROPERTY,
            ConfigDef.Type.STRING,
            null,
            Importance.HIGH,
            "The format of the serialized key and value")
        .define(
            KEY_SCHEMA_ID,
            ConfigDef.Type.INT,
            null,
            Importance.LOW,
            "Undocumented feature"
        ).define(
            VALUE_SCHEMA_ID,
            ConfigDef.Type.INT,
            null,
            Importance.LOW,
            "Undocumented feature"
        ).define(
            SOURCE_TOPIC_CLEANUP_POLICY,
            ConfigDef.Type.STRING,
            null,
            Importance.LOW,
            "This config designates the retention policy to use on log segments. "
                + "The \"delete\" policy (which is the default) will discard old segments "
                + "when their retention time or size limit has been reached. The \"compact\" "
                + "policy will enable <a href=\"#compaction\">log compaction</a>, which retains "
                + "the latest value for each key. It is also possible to specify both policies "
                + "in a comma-separated list (e.g. \"delete,compact\"). In this case, old segments "
                + "will be discarded per the retention time and size configuration, while retained "
                + "segments will be compacted.");
  }

  public static void validateKeyValueFormats(final Map<String, Object> configs) {
    final Object value = configs.get(FORMAT_PROPERTY);
    if (value != null) {
      if (configs.get(KEY_FORMAT_PROPERTY) != null) {
        throw new KsqlException("Cannot supply both '" + KEY_FORMAT_PROPERTY + "' and '"
            + FORMAT_PROPERTY + "' properties, as '" + FORMAT_PROPERTY
            + "' sets both key and value "
            + "formats. Either use just '" + FORMAT_PROPERTY + "', or use '" + KEY_FORMAT_PROPERTY
            + "' and '" + VALUE_FORMAT_PROPERTY + "'.");
      }
      if (configs.get(VALUE_FORMAT_PROPERTY) != null) {
        throw new KsqlException("Cannot supply both '" + VALUE_FORMAT_PROPERTY + "' and '"
            + FORMAT_PROPERTY + "' properties, as '" + FORMAT_PROPERTY
            + "' sets both key and value "
            + "formats. Either use just '" + FORMAT_PROPERTY + "', or use '" + KEY_FORMAT_PROPERTY
            + "' and '" + VALUE_FORMAT_PROPERTY + "'.");
      }
    }

    final Object avroValueSchemaFullName = configs.get(VALUE_AVRO_SCHEMA_FULL_NAME);
    if (avroValueSchemaFullName == null) {
      return;
    }

    final Object valueSchemaFullName = configs.get(VALUE_SCHEMA_FULL_NAME);
    if (valueSchemaFullName != null) {
      throw new KsqlException("Cannot supply both '" + VALUE_AVRO_SCHEMA_FULL_NAME + "' and '"
          + VALUE_SCHEMA_FULL_NAME + "' properties. Please only set '" + VALUE_SCHEMA_FULL_NAME
          + "'.");
    }

  }

  private CommonCreateConfigs() {
  }
}
