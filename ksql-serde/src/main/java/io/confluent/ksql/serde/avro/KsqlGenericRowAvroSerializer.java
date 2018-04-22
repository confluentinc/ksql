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

package io.confluent.ksql.serde.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KsqlGenericRowAvroSerializer implements Serializer<GenericRow> {

  private final Schema avroSchema;
  private final List<Schema.Field> fields;
  private final KafkaAvroSerializer kafkaAvroSerializer;

  public KsqlGenericRowAvroSerializer(
      org.apache.kafka.connect.data.Schema schema,
      SchemaRegistryClient schemaRegistryClient, KsqlConfig
      ksqlConfig
  ) {
    String avroSchemaStr = SchemaUtil.buildAvroSchema(schema, "avro_schema");
    
    Schema.Parser parser = new Schema.Parser();
    avroSchema = parser.parse(avroSchemaStr);
    fields = avroSchema.getFields();

    Map<String, Object> map = new HashMap<>();

    // Automatically register the schema in the Schema Registry if it has not been registered.
    map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
    );
    kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient, map);

  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {

  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }
    try {
      GenericRecord avroRecord = new GenericData.Record(avroSchema);
      for (int i = 0; i < genericRow.getColumns().size(); i++) {
        Schema schema = getNonNullSchema(fields.get(i).schema());
        if (schema.getType() == Schema.Type.ARRAY) {
          if (genericRow.getColumns().get(i) != null) {
            avroRecord.put(
                fields.get(i).name(),
                Arrays.asList((Object[]) genericRow.getColumns().get(i))
            );
          }
        } else {
          avroRecord.put(fields.get(i).name(), genericRow.getColumns().get(i));
        }
      }
      return kafkaAvroSerializer.serialize(topic, avroRecord);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  private Schema getNonNullSchema(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      List<Schema> schemaList = schema.getTypes();
      for (Schema innerSchema: schemaList) {
        if (innerSchema.getType() != Schema.Type.NULL) {
          return innerSchema;
        }
      }
    }
    throw new IllegalStateException("Expecting non-null value or a Union type for " + schema);
  }

  @Override
  public void close() {

  }
}
