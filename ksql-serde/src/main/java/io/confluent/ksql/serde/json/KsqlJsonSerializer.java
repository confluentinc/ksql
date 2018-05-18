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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    for (Field field: schema.fields()) {
      if (field.schema().type() == Schema.Type.STRUCT) {
        Struct structField = updateStructSchema((Struct) struct.get(field.name()), field.schema());
        updatedStruct.put(field.name(), structField);
      } else if (field.schema().type() == Schema.Type.ARRAY) {
        updatedStruct.put(field.name(),
                          updateArraySchema((List) struct.get(field.name()), field.schema()));
      } else if (field.schema().type() == Schema.Type.MAP) {
        updatedStruct.put(field.name(),
                          updateMapSchema((Map) struct.get(field.name()), field.schema()));
      } else {
        updatedStruct.put(field.name(), struct.get(field.name()));
      }
    }

    return updatedStruct;
  }

  private List updateArraySchema(List arrayList, Schema arraySchema) {
    List updatedArray = new ArrayList();
    if (arraySchema.valueSchema().type() == Schema.Type.STRUCT) {
      arrayList.stream().forEach(
          item -> updatedArray.add(updateStructSchema((Struct) item, arraySchema.valueSchema()))
      );
    } else if (arraySchema.valueSchema().type() == Schema.Type.ARRAY) {
      arrayList.stream().forEach(
          item -> updatedArray.add(updateArraySchema((List) item, arraySchema.valueSchema())));
    } else if (arraySchema.valueSchema().type() == Schema.Type.MAP) {
      arrayList.stream().forEach(
          item -> updatedArray.add(updateMapSchema((Map) item, arraySchema.valueSchema())));
    } else {
      arrayList.stream().forEach(item -> updatedArray.add(item));
    }
    return updatedArray;
  }

  private Map updateMapSchema(Map map, Schema mapSchema) {
    Map updatedMap = new HashMap();
    if (mapSchema.valueSchema().type() == Schema.Type.STRUCT) {
      map.entrySet()
          .stream()
          .forEach(entry ->
                       updatedMap.put(((Map.Entry) entry).getKey(),
                                      updateStructSchema((Struct)((Map.Entry) entry).getValue(),
                                                         mapSchema.valueSchema())));
    } else if (mapSchema.valueSchema().type() == Schema.Type.ARRAY) {
      map.entrySet()
          .stream()
          .forEach(entry ->
                       updatedMap.put(((Map.Entry) entry).getKey(),
                                      updateArraySchema((List) ((Map.Entry) entry).getValue(),
                                                        mapSchema.valueSchema())));
    } else if (mapSchema.valueSchema().type() == Schema.Type.MAP) {
      map.entrySet()
          .stream()
          .forEach(entry ->
                       updatedMap.put(((Map.Entry) entry).getKey(),
                                      updateMapSchema((Map) ((Map.Entry) entry).getValue(),
                                                      mapSchema.valueSchema())));
    } else {
      map.entrySet()
          .stream()
          .forEach(entry ->
                       updatedMap.put(((Map.Entry) entry).getKey(),
                                      ((Map.Entry) entry).getValue()));
    }
    return updatedMap;
  }

  private boolean compareSchemas(Schema schema1, Schema schema2) {
    if (schema1.type() != schema2.type()) {
      return false;
    }
    if (schema1.type() == Schema.Type.STRUCT) {
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
    } else if (schema1.type() == Schema.Type.ARRAY) {
      return compareSchemas(schema1.valueSchema(), schema2.valueSchema());
    } else if (schema1.type() == Schema.Type.MAP) {
      return compareSchemas(schema1.valueSchema(), schema2.valueSchema())
             && compareSchemas(schema1.keySchema(), schema2.keySchema());
    }
    return true;
  }

  @Override
  public void close() {
  }

}