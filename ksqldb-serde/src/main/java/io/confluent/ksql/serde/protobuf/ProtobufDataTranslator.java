/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.ksql.serde.protobuf;

import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ProtobufDataTranslator implements DataTranslator {
  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema protoCompatibleSchema;

  ProtobufDataTranslator(final Schema schema, final String schemaFullName) {
    this.ksqlSchema = Objects.requireNonNull(schema, "schema");
    this.protoCompatibleSchema = ProtobufSchemas.schemaWithName(schema, schemaFullName);
    this.innerTranslator = new ConnectDataTranslator(protoCompatibleSchema);
  }

  Schema getProtoCompatibleSchema() {
    return protoCompatibleSchema;
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object protoCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (protoCompatibleRow == null) {
      return null;
    }

    return replaceSchema(ksqlSchema, protoCompatibleRow);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    final Object compatible = replaceSchema(protoCompatibleSchema, ksqlData);
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
      case BYTES:
        if (DecimalUtil.isDecimal(schema)) {
          return DecimalUtil.ensureFit((BigDecimal) object, schema);
        } else {
          return object;
        }
      default:
        return object;
    }
  }
}
