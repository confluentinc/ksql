/*
 * Copyright 2022 Confluent Inc.
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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class ProtobufSchemas {
  private ProtobufSchemas() {
  }

  /**
   * Adds the {@code schemaName} to the provided {@code schema}. The name is only added to the
   * parent schema name to allow Protobuf to extract the name later. Support for nested schema
   * extractions are not supported.
   */
  static Schema schemaWithName(final Schema schema, final String schemaName) {
    if (schemaName == null || schema.type() != Schema.Type.STRUCT) {
      return schema;
    }

    final SchemaBuilder builder = SchemaBuilder.struct();

    for (final Field f : schema.fields()) {
      builder.field(f.name(), f.schema());
    }

    if (schema.parameters() != null) {
      builder.parameters(schema.parameters());
    }

    if (schema.isOptional()) {
      builder.optional();
    }

    if (schema.defaultValue() != null) {
      builder.defaultValue(schema.defaultValue());
    }

    builder.doc(schema.doc());
    builder.version(schema.version());

    return builder.name(schemaName).build();
  }
}
