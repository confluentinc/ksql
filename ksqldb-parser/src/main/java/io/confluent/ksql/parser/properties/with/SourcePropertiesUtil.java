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

package io.confluent.ksql.parser.properties.with;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.delimited.DelimitedFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;

public final class SourcePropertiesUtil {

  private SourcePropertiesUtil() {
  }

  /**
   * The {@code DefaultFormatInjector} ensures that CREATE STREAM and CREATE TABLE statements
   * always contain key and format values by the time they reach the engine for processing.
   * As a result, downstream code can assume these formats are present.
   */
  public static FormatInfo getKeyFormat(
      final CreateSourceProperties properties,
      final SourceName sourceName,
      final KsqlConfig config
  ) {
    return properties.getKeyFormat(sourceName, config)
        .orElseThrow(() -> new IllegalStateException("Key format not present"));
  }

  /**
   * The {@code DefaultFormatInjector} ensures that CREATE STREAM and CREATE TABLE statements
   * always contain key and format values by the time they reach the engine for processing.
   * As a result, downstream code can assume these formats are present.
   */
  public static FormatInfo getValueFormat(
      final CreateSourceProperties properties,
      final KsqlConfig config
  ) {
    return properties.getValueFormat(config)
        .orElseThrow(() -> new IllegalStateException("Value format not present"));
  }

  static Map<String, String> getKeyFormatProperties(
      final PropertiesConfig props,
      final String name,
      final String keyFormat,
      final boolean unwrapProtobufPrimitives
  ) {
    final Builder<String, String> builder = ImmutableMap.builder();
    final String schemaName = props.getString(CommonCreateConfigs.KEY_SCHEMA_FULL_NAME);
    if (schemaName != null) {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, schemaName);
    } else if (AvroFormat.NAME.equalsIgnoreCase(keyFormat)) {
      // ensure that the schema name for the key is unique to the sink - this allows
      // users to always generate valid, non-conflicting avro record definitions in
      // generated Java classes (https://github.com/confluentinc/ksql/issues/6465)
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, AvroFormat.getKeySchemaName(name));
    }

    if (ProtobufFormat.NAME.equalsIgnoreCase(keyFormat) && unwrapProtobufPrimitives) {
      builder.put(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP);
    }

    final String delimiter = props.getString(CommonCreateConfigs.KEY_DELIMITER_PROPERTY);
    if (delimiter != null) {
      builder.put(DelimitedFormat.DELIMITER, delimiter);
    }

    final Optional<Integer> keySchemaId = getKeySchemaId(props);
    keySchemaId.ifPresent(id -> builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  static Map<String, String> getValueFormatProperties(
      final PropertiesConfig props,
      final String valueFormat,
      final boolean unwrapProtobufPrimitives
  ) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    final String avroSchemaName = props.getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME);
    final String schemaName =
        avroSchemaName == null ? props.getString(CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME)
            : avroSchemaName;
    if (schemaName != null) {
      builder.put(ConnectProperties.FULL_SCHEMA_NAME, schemaName);
    }

    if (ProtobufFormat.NAME.equalsIgnoreCase(valueFormat) && unwrapProtobufPrimitives) {
      builder.put(ProtobufProperties.UNWRAP_PRIMITIVES, ProtobufProperties.UNWRAP);
    }

    final String delimiter = props.getString(CommonCreateConfigs.VALUE_DELIMITER_PROPERTY);
    if (delimiter != null) {
      builder.put(DelimitedFormat.DELIMITER, delimiter);
    }

    final Optional<Integer> valueSchemaId = getValueSchemaId(props);
    valueSchemaId.ifPresent(id ->
        builder.put(ConnectProperties.SCHEMA_ID, String.valueOf(id)));

    return builder.build();
  }

  static Optional<Integer> getKeySchemaId(final PropertiesConfig props) {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.KEY_SCHEMA_ID));
  }

  static Optional<Integer> getValueSchemaId(final PropertiesConfig props) {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.VALUE_SCHEMA_ID));
  }
}
