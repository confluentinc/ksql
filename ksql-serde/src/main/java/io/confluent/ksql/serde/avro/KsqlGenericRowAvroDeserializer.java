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
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KsqlGenericRowAvroDeserializer implements Deserializer<GenericRow> {

  private final Schema schema;

  String rowAvroSchemaString;
  org.apache.avro.Schema.Parser parser;
  org.apache.avro.Schema avroSchema;
  GenericDatumReader<GenericRecord> reader;
  KafkaAvroDeserializer kafkaAvroDeserializer;

  public KsqlGenericRowAvroDeserializer(Schema schema,
                                        SchemaRegistryClient schemaRegistryClient) {
    this.schema = schema;
    this.kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
    rowAvroSchemaString = (String) map.get(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG);
    if (rowAvroSchemaString == null) {
      throw new SerializationException("Avro schema is not set for the deserializer.");
    }
    parser = new org.apache.avro.Schema.Parser();
    avroSchema = parser.parse(rowAvroSchemaString);
    reader = new GenericDatumReader<>(avroSchema);
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {

    GenericRecord genericRecord = (GenericRecord) kafkaAvroDeserializer.deserialize(topic, bytes);

    if (bytes == null) {
      return null;
    }

    GenericRow genericRow = null;
    try {
      List columns = new ArrayList();
      for (Field field : schema.fields()) {
        columns.add(enforceFieldType(field.schema(), genericRecord.get(field.name())));
      }
      genericRow = new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
    return genericRow;
  }

  private byte[] removeSchemaRegistryMetaBytes(final byte[] data) {
    byte[] avroBytes = new byte[data.length - 5];
    for (int i = 5; i < data.length; i++) {
      avroBytes[i-5] = data[i];
    }
    return avroBytes;
  }

  private Object enforceFieldType(Schema fieldSchema, Object value) {

    switch (fieldSchema.type()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT64:
      case STRING:
      case MAP:
        return value;
      case ARRAY:
        GenericData.Array genericArray = (GenericData.Array) value;
        Class elementClass = SchemaUtil.getJavaType(fieldSchema.valueSchema());
        Object[] arrayField =
            (Object[]) java.lang.reflect.Array.newInstance(elementClass, genericArray.size());
        for (int i = 0; i < genericArray.size(); i++) {
          Object obj = enforceFieldType(fieldSchema.valueSchema(), genericArray.get(i));
          arrayField[i] = obj;
        }
        return arrayField;
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.schema());

    }
  }


  @Override
  public void close() {

  }
}
