/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslator;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Translates KSQL data and schemas to Avro equivalents.
 *
 * <p>Responsible for converting the KSQL schema to a version ready for connect to convert to an
 * avro schema.
 *
 * <p>This includes ensuring field names are valid Avro field names and that nested types do not
 * have name clashes.
 */
public class AvroSRSchemaDataTranslator extends ConnectSRSchemaDataTranslator {

  AvroSRSchemaDataTranslator(final Schema schema) {
    super(schema);
  }

  private void copyArray(
      final List originalData,
      final Schema originalSchema,
      final List array,
      final Schema schema) {
    for (Object field : originalData) {
      if (field instanceof List) {
        final List nestedArray = new ArrayList();
        copyArray((List) field, originalSchema.valueSchema(), nestedArray, schema.valueSchema());
        array.add(nestedArray);
      } else if (field instanceof Map) {
        final Map nestedMap = new HashMap();
        copyMap((Map) field, originalSchema.valueSchema(), nestedMap, schema.valueSchema());
        array.add(nestedMap);
      } else if (field instanceof Struct) {
        final Struct innerStruct = new Struct(schema.valueSchema());
        copyStruct((Struct) field, ((Struct) field).schema(), innerStruct, schema.valueSchema());
        array.add(innerStruct);
      } else {
        array.add(field);
      }
    }
  }

  private void copyMap(
      final Map<String, Object> originalData,
      final Schema originalSchema,
      final Map map,
      final Schema schema) {
    for (Map.Entry<String, Object> entry : originalData.entrySet()) {
      final String key = entry.getKey(); // KSQL supports only string keys for Map
      final Object value = entry.getValue();
      if (value instanceof List) {
        final List nestedArray = new ArrayList();
        copyArray((List) value, originalSchema.valueSchema(), nestedArray, schema.valueSchema());
        map.put(key, nestedArray);
      } else if (value instanceof Map) {
        final Map nestedMap = new HashMap();
        copyMap((Map) value, originalSchema.valueSchema(), nestedMap, schema.valueSchema());
        map.put(key, nestedMap);
      } else if (value instanceof Struct) {
        final Struct innerStruct = new Struct(schema.valueSchema());
        copyStruct((Struct) value, ((Struct) value).schema(), innerStruct, schema.valueSchema());
        map.put(key, innerStruct);
      } else {
        map.put(key, value);
      }
    }
  }

  private void copyStruct(
      final Struct originalData,
      final Schema originalSchema,
      final Struct struct,
      final Schema schema) {
    for (Field field : schema.fields()) {
      final Optional<Field> originalField = originalSchema.fields().stream()
          .filter(f -> field.name().equals(f.name())).findFirst();
      if (originalField.isPresent()) {
        final Object data = originalData.get(field);
        final Schema dataSchema = originalField.get().schema();
        if (data instanceof List) {
          final List array = new ArrayList();
          copyArray((List) data, dataSchema, array, field.schema());
          struct.put(field, array);
        } else if (data instanceof Map) {
          final Map map = new HashMap();
          copyMap((Map) data, dataSchema, map, field.schema());
          struct.put(field, map);
        } else if (data instanceof Struct) {
          final Struct innerStruct = new Struct(field.schema());
          copyStruct((Struct) data, dataSchema, innerStruct, field.schema());
          struct.put(field, innerStruct);
        } else {
          struct.put(field, data);
        }
      } else {
        addDefaultValueOrThrowException(field, struct);
      }
    }
  }

  private void addDefaultValueOrThrowException(final Field field, final Struct struct) {
    if (field.schema().defaultValue() != null || field.schema().isOptional()) {
      struct.put(field, field.schema().defaultValue());
    } else {
      throw new KsqlException("Missing default value for required Avro field: [" + field.name()
          + "]. This field appears in Avro schema in Schema Registry");
    }
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    if (!(ksqlData instanceof Struct)) {
      return ksqlData;
    }

    final Schema schema = getSchema();
    final Struct struct = new Struct(schema);
    Struct originalData = (Struct) ksqlData;
    Schema originalSchema = originalData.schema();
    if (originalSchema.name() == null && schema.name() != null) {
      originalSchema = AvroSchemas.getAvroCompatibleConnectSchema(
          originalSchema, schema.name()
      );
      originalData = ConnectSchemas.withCompatibleRowSchema(originalData, originalSchema);
    }

    validate(originalSchema, schema);

    copyStruct(originalData, originalSchema, struct, schema);

    return struct;
  }

}