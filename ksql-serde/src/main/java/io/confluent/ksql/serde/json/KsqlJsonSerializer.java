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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.Collections;
import java.util.Map;

public class KsqlJsonSerializer implements Serializer<GenericRow> {

  private final Schema schema;
  private final JsonConverter jsonConverter;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonSerializer(final Schema schema) {
    this.schema = schema;
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
        struct.put(schema.fields().get(i), data.getColumns().get(i));
      }

      return jsonConverter.fromConnectData(topic, schema, struct);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
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