/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslator;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ProtobufSRSchemaDataTranslator extends ConnectSRSchemaDataTranslator {
  public ProtobufSRSchemaDataTranslator(final Schema schema) {
    super(schema);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    if (!(ksqlData instanceof Struct)) {
      return ksqlData;
    }

    return convertStruct((Struct) ksqlData, getSchema());
  }

  private Struct convertStruct(final Struct source, final Schema targetSchema) {
    final Struct struct = new Struct(targetSchema);
    final Schema originalSchema = source.schema();

    validate(originalSchema, targetSchema);

    for (final Field field : targetSchema.fields()) {
      final Optional<Field> originalField = originalSchema.fields().stream()
          .filter(f -> field.name().equals(f.name())).findFirst();

      if (originalField.isPresent()) {
        final Object originalValue = source.get(originalField.get());
        struct.put(field, ConnectSchemas.withCompatibleSchema(field.schema(), originalValue));
      } else {
        if (field.schema().defaultValue() != null || field.schema().isOptional()) {
          struct.put(field, field.schema().defaultValue());
        } else {
          throw new KsqlException("Missing default value for required Protobuf field: "
              + "[" + field.name() + "]. This field appears in Protobuf schema in Schema Registry");
        }
      }
    }

    return struct;
  }
}
