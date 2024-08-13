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
import java.util.Iterator;
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

  Schema getSchema() {
    return protoCompatibleSchema;
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object protoCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (protoCompatibleRow == null) {
      return null;
    }

    return objectWithSchema(ksqlSchema, protoCompatibleRow);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    final Object compatible = objectWithSchema(protoCompatibleSchema, ksqlData);
    return innerTranslator.toConnectRow(compatible);
  }

  private static Object objectWithSchema(final Schema schema, final Object object) {
    if (object == null || schema.type() != Schema.Type.STRUCT) {
      return object;
    }

    final Struct source = (Struct)object;
    final Struct struct = new Struct(schema);

    final Iterator<Field> sourceIt = source.schema().fields().iterator();

    for (final Field targetField : schema.fields()) {
      final Field sourceField = sourceIt.next();
      final Object value = source.get(sourceField);

      struct.put(targetField, value);
    }

    return struct;
  }
}