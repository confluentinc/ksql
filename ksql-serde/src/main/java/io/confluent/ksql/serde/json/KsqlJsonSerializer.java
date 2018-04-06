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
import java.util.HashMap;
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
        struct.put(schema.fields().get(i).name(), data.getColumns().get(i));
      }

      JsonConverter jsonConverter = new JsonConverter();
      jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
      byte[] serialized = jsonConverter.fromConnectData(topic, schema, struct);
      return serialized;
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  private Map<String, Object> dataToMap(final GenericRow data) {
    if (data == null) {
      return null;
    }
    Map<String, Object> result = new HashMap<>();

    for (int i = 0; i < data.getColumns().size(); i++) {
      String schemaColumnName = schema.fields().get(i).name();
      String mapColumnName = schemaColumnName.substring(schemaColumnName.indexOf('.') + 1);
      result.put(mapColumnName, data.getColumns().get(i));
    }

    return result;
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

  @Override
  public void close() {
  }

}
