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

package io.confluent.ksql.datagen;

import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class DataGenSchemaUtil {

  private DataGenSchemaUtil() {
  }

  public static ConnectSchema getOptionalSchema(final Schema schema) {
    switch (schema.type()) {
      case BOOLEAN:
        return (ConnectSchema) Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case INT32:
        return (ConnectSchema) Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return (ConnectSchema) Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT64:
        return (ConnectSchema) Schema.OPTIONAL_FLOAT64_SCHEMA;
      case STRING:
        return (ConnectSchema) Schema.OPTIONAL_STRING_SCHEMA;
      case ARRAY:
        return (ConnectSchema) SchemaBuilder
            .array(getOptionalSchema(schema.valueSchema()))
            .optional()
            .build();
      case MAP:
        return (ConnectSchema) SchemaBuilder
            .map(
                getOptionalSchema(schema.keySchema()),
                getOptionalSchema(schema.valueSchema())
            )
            .optional()
            .build();
      case STRUCT:
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (final Field field : schema.fields()) {
          schemaBuilder.field(field.name(), getOptionalSchema(field.schema()));
        }
        return (ConnectSchema) schemaBuilder
            .optional()
            .build();
      default:
        throw new KsqlException("Unsupported type: " + schema);
    }
  }

  static Object getOptionalValue(final Schema schema, final Object value) {
    switch (schema.type()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT64:
      case STRING:
        return value;
      case ARRAY:
        final List<?> list = (List<?>) value;
        return list.stream().map(listItem -> getOptionalValue(schema.valueSchema(), listItem))
            .collect(Collectors.toList());
      case MAP:
        final Map<?, ?> map = (Map<?, ?>) value;
        return map.entrySet().stream()
            .collect(Collectors.toMap(
                k -> getOptionalValue(schema.keySchema(), k),
                v -> getOptionalValue(schema.valueSchema(), v)
            ));
      case STRUCT:
        final Struct struct = (Struct) value;
        final Struct optionalStruct = new Struct(getOptionalSchema(schema));
        for (final Field field : schema.fields()) {
          optionalStruct
              .put(field.name(), getOptionalValue(field.schema(), struct.get(field.name())));
        }
        return optionalStruct;

      default:
        throw new KsqlException("Invalid value schema: " + schema + ", value = " + value);
    }
  }

}
