/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class AvroDataTranslator implements DataTranslator {

  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema avroCompatibleSchema;

  AvroDataTranslator(
      final Schema schema,
      final String schemaFullName
  ) {
    this.ksqlSchema = AvroUtil
        .throwOnInvalidSchema(Objects.requireNonNull(schema, "schema"));

    this.avroCompatibleSchema = AvroSchemas.getAvroCompatibleConnectSchema(
        schema, schemaFullName
    );

    this.innerTranslator = new ConnectDataTranslator(avroCompatibleSchema);
  }

  Schema getAvroCompatibleSchema() {
    return avroCompatibleSchema;
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object avroCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (avroCompatibleRow == null) {
      return null;
    }

    return replaceSchema(ksqlSchema, avroCompatibleRow);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    final Object compatible = replaceSchema(avroCompatibleSchema, ksqlData);
    return innerTranslator.toConnectRow(compatible);
  }

  private static Struct convertStruct(
      final Struct source,
      final Schema targetSchema
  ) {
    final Struct struct = new Struct(targetSchema);

    final Iterator<Field> sourceIt = source.schema().fields().iterator();

    for (final Field targetField : targetSchema.fields()) {
      final Field sourceField = sourceIt.next();
      final Object value = source.get(sourceField);
      final Object adjusted = replaceSchema(targetField.schema(), value);
      struct.put(targetField, adjusted);
    }

    return struct;
  }

  @SuppressWarnings("unchecked")
  private static Object replaceSchema(final Schema schema, final Object object) {
    if (object == null) {
      return null;
    }
    switch (schema.type()) {
      case ARRAY:
        final List<Object> ksqlArray = new ArrayList<>(((List) object).size());
        ((List) object).forEach(
            e -> ksqlArray.add(replaceSchema(schema.valueSchema(), e)));
        return ksqlArray;

      case MAP:
        final Map<Object, Object> ksqlMap = new HashMap<>();
        ((Map<Object, Object>) object).forEach(
            (key, value) -> ksqlMap.put(
                replaceSchema(schema.keySchema(), key),
                replaceSchema(schema.valueSchema(), value)
            ));
        return ksqlMap;

      case STRUCT:
        return convertStruct((Struct) object, schema);

      default:
        return object;
    }
  }
}
