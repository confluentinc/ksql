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

  static Schema schemaWithName(final Schema schema, final String schemaName) {
    if (schemaName == null || schema.type() != Schema.Type.STRUCT) {
      return schema;
    }

    final SchemaBuilder builder = buildSchemaStruct(schema);

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

  private static SchemaBuilder buildSchemaStruct(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (final Field f : schema.fields()) {
      schemaBuilder.field(f.name(), f.schema());
    }

    return schemaBuilder;
  }
}
