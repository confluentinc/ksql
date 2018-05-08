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

package io.confluent.ksql.serde.json;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.Collections;
import java.util.Map;

public class KsqlJsonSerializer implements Serializer<GenericRow> {

  private final Schema schema;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonSerializer(Schema schema) {
    this.schema = makeOptional(schema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow data) {
    if (data == null) {
      return null;
    }
    try {
      Struct struct = new Struct(schema);
      for (int i = 0; i < data.getColumns().size(); i++) {
        if (schema.fields().get(i).schema().type() == Schema.Type.STRUCT) {
          struct.put(schema.fields().get(i), updateStructSchema(
              (Struct) data.getColumns().get(i),
              schema.fields().get(i).schema()));
        } else {
          struct.put(schema.fields().get(i), data.getColumns().get(i));
        }

      }

      JsonConverter jsonConverter = new JsonConverter();
      jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
      byte[] serialized = jsonConverter.fromConnectData(topic, schema, struct);
      return serialized;
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  private Schema makeOptional(Schema schema) {
    switch (schema.type()) {
      case BOOLEAN:
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case STRING:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case ARRAY:
        return SchemaBuilder.array(makeOptional(schema.valueSchema())).optional().build();
      case MAP:
        return SchemaBuilder.map(schema.keySchema(), makeOptional(schema.valueSchema()))
            .optional().build();
      case STRUCT:
        SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        schema.fields()
            .stream()
            .forEach(field -> schemaBuilder.field(field.name(), makeOptional(field.schema())));
        return schemaBuilder.optional().build();
      default:
        throw new KsqlException("Invalid type in schema: " + schema);
    }
  }

  /**
   * Temporary work around until schema comparison is fixed.
   *
   */
  private Struct updateStructSchema(Struct struct, Schema schema) {
    if (!compareSchemas(schema, struct.schema())) {
      throw new KsqlException("Incompatible schemas: " + schema + ", " + struct.schema());
    }
    Struct updatedStruct = new Struct(schema);
    schema.fields().stream()
        .forEach(field -> updatedStruct.put(field.name(), struct.get(field.name())));
    return updatedStruct;
  }

  private boolean compareSchemas(Schema schema1, Schema schema2) {
    if (schema1.fields().size() != schema2.fields().size()
        || schema1.type() != schema2.type()) {
      return false;
    }

    for (int i = 0; i < schema1.fields().size(); i++) {
      if (!compareSchemas(schema1.fields().get(i).schema(), schema2.fields().get(i).schema())
          || !schema1.fields().get(i).name().equalsIgnoreCase(schema2.fields().get(i).name())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
  }

}
