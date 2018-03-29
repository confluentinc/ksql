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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

public class KsqlGenericRowAvroDeserializer implements Deserializer<GenericRow> {

  private final Schema schema;

  private KafkaAvroDeserializer kafkaAvroDeserializer;

  public KsqlGenericRowAvroDeserializer(
      Schema schema,
      SchemaRegistryClient schemaRegistryClient,
      boolean isInternal
  ) {
    this(schema, new KafkaAvroDeserializer(schemaRegistryClient), isInternal);
  }

  KsqlGenericRowAvroDeserializer(
      Schema schema,
      KafkaAvroDeserializer kafkaAvroDeserializer,
      boolean isInternal
  ) {
    if (isInternal) {
      this.schema = SchemaUtil.getAvroSerdeKsqlSchema(schema);
    } else {
      this.schema = SchemaUtil.getSchemaWithNoAlias(schema);
    }

    this.kafkaAvroDeserializer = kafkaAvroDeserializer;

  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {

    if (bytes == null) {
      return null;
    }

    GenericRow genericRow;
    try {
      GenericRecord genericRecord = (GenericRecord) kafkaAvroDeserializer.deserialize(topic, bytes);
      Map<String, String> caseInsensitiveFieldNameMap = getCaseInsensitiveFieldMap(genericRecord);
      List columns = new ArrayList();
      for (Field field : schema.fields()) {
        // Set the missing fields to null. We can make this configurable later.
        if (genericRecord.get(caseInsensitiveFieldNameMap.get(field.name().toUpperCase()))
            == null) {
          columns.add(null);
        } else {
          columns.add(
              enforceFieldType(
                  field.schema(),
                  genericRecord.get(caseInsensitiveFieldNameMap.get(field.name().toUpperCase()))
              )
          );
        }

      }
      genericRow = new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
    return genericRow;
  }

  @SuppressWarnings("unchecked")
  private Object enforceFieldType(Schema fieldSchema, Object value) {

    switch (fieldSchema.type()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT64:
        return value;
      case STRING:
        if (value != null) {
          return value.toString();
        } else {
          return value;
        }

      case ARRAY:
        return handleArray(fieldSchema, (GenericData.Array) value);
      case MAP:
        return handleMap(fieldSchema, (Map) value);
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.schema());

    }
  }

  @SuppressWarnings("unchecked")
  private Object handleMap(Schema fieldSchema, Map valueMap) {
    Map<String, Object> ksqlMap = new HashMap<>();
    Set<Map.Entry> entrySet = valueMap.entrySet();
    for (Map.Entry avroMapEntry : entrySet) {
      ksqlMap.put(
          avroMapEntry.getKey().toString(),
          enforceFieldType(fieldSchema.valueSchema(), avroMapEntry.getValue())
      );
    }
    return ksqlMap;
  }

  private Object handleArray(Schema fieldSchema, GenericData.Array genericArray) {
    Class elementClass = SchemaUtil.getJavaType(fieldSchema.valueSchema());
    Object[] arrayField =
        (Object[]) java.lang.reflect.Array.newInstance(elementClass, genericArray.size());
    for (int i = 0; i < genericArray.size(); i++) {
      Object obj = enforceFieldType(fieldSchema.valueSchema(), genericArray.get(i));
      arrayField[i] = obj;
    }
    return arrayField;
  }

  Map<String, String> getCaseInsensitiveFieldMap(GenericRecord genericRecord) {
    Map<String, String> fieldMap = new HashMap<>();
    for (org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
      fieldMap.put(field.name().toUpperCase(), field.name());
    }
    return fieldMap;
  }

  @Override
  public void close() {

  }
}
