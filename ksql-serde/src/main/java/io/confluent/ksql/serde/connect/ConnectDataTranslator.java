/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectDataTranslator {
  public GenericRow toKsqlRow(final Schema schema, final Schema connectSchema,
                              final Object connectData) {
    if (! schema.type().equals(Schema.Type.STRUCT)) {
      throw new KsqlException("Schema for a KSQL row should be a struct");
    }
    final Struct rowStruct = (Struct) toKsqlValue(schema, connectSchema, connectData);
    return new GenericRow(
        schema.fields()
            .stream()
            .map(f -> rowStruct.get(f.name()))
            .collect(Collectors.toList())
    );
  }

  private Object toKsqlValue(final Schema schema, final Schema connectSchema,
                             final Object connectValue) {
    // Map a connect value+schema onto the schema expected by KSQL. For now this involves:
    // - handling case insensitivity for struct field names
    // - setting missing values to null

    if (!schema.type().equals(connectSchema.type())) {
      throw new DataException(
          String.format(
              "Cannot deserialize type %s as type %s",
              connectSchema.type().getName(),
              schema.type().getName()));
    }
    if (connectValue == null) {
      return null;
    }
    if (schema.type().isPrimitive()) {
      return connectValue;
    }
    if (schema.type().equals(Schema.Type.ARRAY)) {
      return toKsqlArray(schema.valueSchema(), connectSchema.valueSchema(), (List) connectValue);
    }
    if (schema.type().equals(Schema.Type.MAP)) {
      return toKsqlMap(
          schema.keySchema(), connectSchema.keySchema(),
          schema.valueSchema(), connectSchema.valueSchema(), (Map) connectValue);
    }
    return toKsqlStruct(schema, connectSchema, (Struct) connectValue);
  }

  private List toKsqlArray(final Schema valueSchema, final Schema connectValueSchema,
                           final List<Object> connectArray) {
    return connectArray.stream()
        .map(o -> toKsqlValue(valueSchema, connectValueSchema, o))
        .collect(Collectors.toList());
  }

  private Map toKsqlMap(final Schema keySchema, final Schema connectKeySchema,
                        final Schema valueSchema, final Schema connectValueSchema,
                        final Map<String, Object> connectMap) {
    return connectMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                e -> toKsqlValue(keySchema, connectKeySchema, e.getKey()),
                e -> toKsqlValue(valueSchema, connectValueSchema, e.getValue())
            ));
  }

  private Struct toKsqlStruct(final Schema schema, final Schema connectSchema,
                              final Struct connectStruct) {
    // todo: check name here? e.g. what if the struct gets changed to a union?
    final Struct ksqlStruct = new Struct(schema);
    final Map<String, String> caseInsensitiveFieldNameMap
        = getCaseInsensitiveFieldMap(connectStruct.schema());
    for (Field field : schema.fields()) {
      final String fieldNameUppercase = field.name().toUpperCase();
      // TODO: should we throw an exception if this is not true? this means the schema changed
      //       or the user declared the source with a schema incompatible with the registry schema
      if (caseInsensitiveFieldNameMap.containsKey(fieldNameUppercase)) {
        final Object fieldValue = connectStruct.get(
            caseInsensitiveFieldNameMap.get(fieldNameUppercase));
        final Schema fieldSchema = connectSchema.field(
            caseInsensitiveFieldNameMap.get(fieldNameUppercase)).schema();
        ksqlStruct.put(field.name(), toKsqlValue(field.schema(), fieldSchema, fieldValue));
      }
    }
    return ksqlStruct;
  }

  private Map<String, String> getCaseInsensitiveFieldMap(final Schema schema) {
    return schema.fields()
        .stream()
        .collect(
            Collectors.toMap(
                f -> f.name().toUpperCase(),
                Field::name));
  }
}
