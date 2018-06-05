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

import java.util.stream.Collectors;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KsqlJsonSerializer implements Serializer<GenericRow> {

  private final Schema schema;
  private final JsonConverter jsonConverter;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonSerializer(final Schema schema) {
    this.schema = makeOptional(schema);
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
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
      final Struct struct = new Struct(schema);
      for (int i = 0; i < data.getColumns().size(); i++) {
        Schema structSchema = schema.fields().get(i).schema();
        if (structSchema.type() == Schema.Type.STRUCT) {
          Field structField = schema.fields().get(i);
          struct.put(structField, updateStructSchema(
              (Struct) data.getColumns().get(i),
              structField.schema()));
        } else {
          struct.put(schema.fields().get(i), data.getColumns().get(i));
        }
      }

      return jsonConverter.fromConnectData(topic, schema, struct);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  private Schema makeOptional(final Schema schema) {
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
        final SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        schema.fields()
            .forEach(field -> schemaBuilder.field(field.name(), makeOptional(field.schema())));
        return schemaBuilder.optional().build();
      default:
        throw new KsqlException("Invalid type in schema: " + schema);
    }
  }

  /**
   * Temporary work around until schema comparison is fixed.
   * Currently, when we put a value in a struct the schema object should match too
   * however, although the schemas are the same but the objects do not match.
   * This is to overcome this problem.
   */
  private Struct updateStructSchema(final Struct struct, final Schema schema) {
    if (!compareSchemas(schema, struct.schema())) {
      throw new KsqlException("Incompatible schemas: " + schema + ", " + struct.schema());
    }
    Struct updatedStruct = new Struct(schema);
    for (Field field : schema.fields()) {
      switch (field.schema().type()) {
        case STRUCT:
          updatedStruct.put(field.name(),
              updateStructSchema((Struct) struct.get(field.name()), field.schema()));
          break;
        case ARRAY:
          updatedStruct.put(field.name(),
              updateArraySchema((List) struct.get(field.name()), field.schema()));
          break;
        case MAP:
          updatedStruct.put(field.name(),
              updateMapSchema((Map) struct.get(field.name()), field.schema()));
          break;
        default:
          updatedStruct.put(field.name(), struct.get(field.name()));
      }
    }

    return updatedStruct;
  }

  private List<?> updateArraySchema(List<?> elements, Schema arraySchema) {
    switch (arraySchema.valueSchema().type()) {
      case STRUCT:
        return elements.stream()
            .map(item -> updateStructSchema((Struct) item, arraySchema.valueSchema()))
            .collect(Collectors.toList());
      case ARRAY:
        return elements.stream()
            .map(item -> updateArraySchema((List) item, arraySchema.valueSchema()))
            .collect(Collectors.toList());
      case MAP:
        return elements.stream()
            .map(item -> updateMapSchema((Map) item, arraySchema.valueSchema()))
            .collect(Collectors.toList());
      default:
        return elements;
    }
  }

  private Map updateMapSchema(final Map map, final Schema mapSchema) {
    switch (mapSchema.valueSchema().type()) {
      case STRUCT:
        return (Map) map.entrySet().stream()
            .collect(Collectors.toMap(
                e -> ((Map.Entry) e).getKey(),
                e -> updateStructSchema((Struct) ((Map.Entry) e).getValue(),
                    mapSchema.valueSchema())
            ));
      case ARRAY:
        return (Map) map.entrySet().stream()
            .collect(Collectors.toMap(
                e -> ((Map.Entry) e).getKey(),
                e -> updateArraySchema((List) ((Map.Entry) e).getValue(),
                    mapSchema.valueSchema())
            ));
      case MAP:
        return (Map) map.entrySet().stream()
            .collect(Collectors.toMap(
                e -> ((Map.Entry) e).getKey(),
                e -> updateMapSchema((Map) ((Map.Entry) e).getValue(),
                    mapSchema.valueSchema())
            ));
      default:
        return (Map) map.entrySet().stream()
            .collect(Collectors.toMap(
                e -> ((Map.Entry) e).getKey(),
                e -> ((Map.Entry) e).getValue()
            ));
    }
  }

  private boolean compareSchemas(final Schema schema1, final Schema schema2) {
    if (schema1.type() != schema2.type()) {
      return false;
    }

    switch (schema1.type()) {
      case STRUCT:
        return compareStructSchema(schema1, schema2);
      case ARRAY:
        return compareSchemas(schema1.valueSchema(), schema2.valueSchema());
      case MAP:
        return compareSchemas(schema1.valueSchema(), schema2.valueSchema())
            && compareSchemas(schema1.keySchema(), schema2.keySchema());
      default:
        return true;
    }
  }

  private boolean compareStructSchema(Schema schema1, Schema schema2) {
    if (schema1.fields().size() != schema2.fields().size()) {
      return false;
    }
    for (int i = 0; i < schema1.fields().size(); i++) {
      if (!schema1.fields().get(i).name().equalsIgnoreCase(schema2.fields().get(i).name())
          || !compareSchemas(schema1.fields().get(i).schema(),
          schema2.fields().get(i).schema())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
  }

}