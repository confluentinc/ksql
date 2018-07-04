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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;

public class TypeUtil {

  public static Type getKsqlType(Schema schema) {
    switch (schema.type()) {
      case INT32:
        return new PrimitiveType(Type.KsqlType.INTEGER);
      case INT64:
        return new PrimitiveType(Type.KsqlType.BIGINT);
      case FLOAT32:
      case FLOAT64:
        return new PrimitiveType(Type.KsqlType.DOUBLE);
      case BOOLEAN:
        return new PrimitiveType(Type.KsqlType.BOOLEAN);
      case STRING:
        return new PrimitiveType(Type.KsqlType.STRING);
      case ARRAY:
        return new Array(getKsqlType(schema.valueSchema()));
      case MAP:
        return new Map(getKsqlType(schema.valueSchema()));
      case STRUCT:
        return new Struct(getStructItems(schema));

      default:
        throw new KsqlException(String.format("Invalid type in schema: %s.", schema.toString()));
    }
  }

  private static List<Pair<String, Type>> getStructItems(Schema struct) {
    if (struct.type() != Schema.Type.STRUCT) {
      return null;
    }
    List<Pair<String, Type>> itemList = new ArrayList<>();
    for (Field field: struct.schema().fields()) {
      itemList.add(new Pair<>(field.name(), getKsqlType(field.schema())));
    }
    return itemList;
  }

  public static Schema getTypeSchema(final Type ksqlType) {
    switch (ksqlType.getKsqlType()) {
      case BOOLEAN:
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case INTEGER:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case BIGINT:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case DOUBLE:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case STRING:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case ARRAY:
        return SchemaBuilder.array(
          getTypeSchema(((Array) ksqlType).getItemType())
          ).optional().build();
      case MAP:
        return SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA,
                                 getTypeSchema(((Map) ksqlType).getValueType())).optional().build();
      case STRUCT:
        return buildStructSchema((Struct) ksqlType);

      default:
        throw new KsqlException("Invalid ksql type: " + ksqlType);
    }
  }

  private static Schema buildStructSchema(final Struct struct) {
    final SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
    for (Pair<String, Type> field: struct.getItems()) {
      structSchemaBuilder.field(field.getLeft(), getTypeSchema(field.getRight()));
    }
    return structSchemaBuilder.optional().build();
  }
}
