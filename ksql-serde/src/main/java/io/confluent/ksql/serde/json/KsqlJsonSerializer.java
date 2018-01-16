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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;

public class KsqlJsonSerializer implements Serializer<GenericRow> {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Schema schema;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonSerializer(Schema schema) {
    this.schema = schema;
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
      return objectMapper.writeValueAsBytes(dataToMap(data));
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

  @Override
  public void close() {
  }

}
