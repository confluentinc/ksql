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

package io.confluent.ksql.serde.protobuf;

import static io.confluent.connect.protobuf.ProtobufDataConfig.WRAPPER_FOR_RAW_PRIMITIVES_CONFIG;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

/**
 * Translates between Connect and Protobuf Schema Registry schema types.
 */
public class ProtobufSchemaTranslator implements ConnectSchemaTranslator {
  private static final String DEFAULT_PREFIX_SCHEMA_NAME = "ConnectDefault";

  private final Map<String, Object> baseConfigs;
  private final Optional<String> fullNameSchema;

  private Map<String, Object> updatedConfigs;
  private ProtobufData protobufData;

  public ProtobufSchemaTranslator(final ProtobufProperties properties) {
    Objects.requireNonNull(properties, "properties");

    this.baseConfigs = ImmutableMap.of(
        WRAPPER_FOR_RAW_PRIMITIVES_CONFIG, properties.getUnwrapPrimitives(),

        // This flag is needed so that the schema translation in toConnectRow() adds the
        // package name information to the row schema
        ProtobufDataConfig.ENHANCED_PROTOBUF_SCHEMA_SUPPORT_CONFIG, "true"
    );
    this.fullNameSchema = ignoreDefaultName(properties.getFullSchemaName());
    this.updatedConfigs = baseConfigs;
    this.protobufData = new ProtobufData(new ProtobufDataConfig(baseConfigs));
  }

  private Optional<String> ignoreDefaultName(final String fullNameSchema) {
    if (fullNameSchema == null) {
      return Optional.empty();
    }

    final String trimmedName = fullNameSchema.trim();
    if (trimmedName.startsWith(DEFAULT_PREFIX_SCHEMA_NAME)) {
      // Ignore the fullSchemaName if a default name is used. Otherwise, if the parent schema
      // is named with this name by ProtobufSchemas, then the fromConnectSchema() starts the
      // default name consecutive number in the nested structs, not the parent, which causes
      // serializers to fail with incompatible schema.
      return Optional.empty();
    }

    return Optional.ofNullable(Strings.emptyToNull(trimmedName));
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    final Map<String, Object> mergedConfigs = new HashMap<>(configs);
    mergedConfigs.putAll(baseConfigs);

    updatedConfigs = mergedConfigs;
    protobufData = new ProtobufData(new ProtobufDataConfig(mergedConfigs));
  }

  @Override
  public String name() {
    return ProtobufSchema.TYPE;
  }

  @Override
  public Schema toConnectSchema(final ParsedSchema schema) {
    return protobufData.toConnectSchema(withSchemaFullName((ProtobufSchema) schema));
  }

  /**
   * Converts a Connect schema to a SR schema. The conversion uses the full schema name
   * (if provided during initialization) to name the SR schema as well as extract a
   * single schema definition if multiple schema definitions are found in {@code schema}.
   */
  @Override
  public ParsedSchema fromConnectSchema(final Schema schema) {
    // Bug in ProtobufData means `fromConnectSchema` throws on the second invocation if using
    // default naming.
    return new ProtobufData(new ProtobufDataConfig(updatedConfigs))
        .fromConnectSchema(injectSchemaFullName(schema));
  }

  private ProtobufSchema withSchemaFullName(final ProtobufSchema origSchema) {
    return fullNameSchema.map(origSchema::copy).orElse(origSchema);
  }

  private Schema injectSchemaFullName(final Schema origSchema) {
    return fullNameSchema
        .map(fullName -> ProtobufSchemas.schemaWithName(origSchema, fullName))
        .orElse(origSchema);
  }
}
