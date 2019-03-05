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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Type.SqlType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class TypeUtil {

  private static final Map<Schema.Type, Function<Schema, Type>> KSQL_TYPES = ImmutableMap
      .<Schema.Type, Function<Schema, Type>>builder()
      .put(Schema.Type.INT32, s -> new PrimitiveType(SqlType.INTEGER))
      .put(Schema.Type.INT64, s -> new PrimitiveType(SqlType.BIGINT))
      .put(Schema.Type.FLOAT32, s -> new PrimitiveType(SqlType.DOUBLE))
      .put(Schema.Type.FLOAT64, s -> new PrimitiveType(SqlType.DOUBLE))
      .put(Schema.Type.BOOLEAN, s -> new PrimitiveType(SqlType.BOOLEAN))
      .put(Schema.Type.STRING, s -> new PrimitiveType(SqlType.STRING))
      .put(Schema.Type.ARRAY, TypeUtil::toKsqlArray)
      .put(Schema.Type.MAP, TypeUtil::toKsqlMap)
      .put(Schema.Type.STRUCT, TypeUtil::toKsqlStruct)
      .build();

  private TypeUtil() {
  }

  public static Type getKsqlType(final Schema schema) {
    final Function<Schema, Type> handler = KSQL_TYPES.get(schema.type());
    if (handler == null) {
      throw new KsqlException(String.format("Invalid type in schema: %s.", schema));
    }

    return handler.apply(schema);
  }

  private static Array toKsqlArray(final Schema schema) {
    return new Array(getKsqlType(schema.valueSchema()));
  }

  private static Type toKsqlMap(final Schema schema) {
    if (schema.keySchema().type() != Schema.Type.STRING) {
      throw new KsqlException("Unsupported map key type in schema: " + schema.keySchema());
    }
    return new io.confluent.ksql.parser.tree.Map(getKsqlType(schema.valueSchema()));
  }

  private static Struct toKsqlStruct(final Schema schema) {
    return new Struct(getStructItems(schema));
  }

  private static List<Pair<String, Type>> getStructItems(final Schema struct) {
    if (struct.type() != Schema.Type.STRUCT) {
      return null;
    }
    final List<Pair<String, Type>> itemList = new ArrayList<>();
    for (final Field field: struct.schema().fields()) {
      itemList.add(new Pair<>(field.name(), getKsqlType(field.schema())));
    }
    return itemList;
  }

  public static Schema getTypeSchema(final Type ksqlType) {
    switch (ksqlType.getSqlType()) {
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
            getTypeSchema(((io.confluent.ksql.parser.tree.Map) ksqlType).getValueType())).optional()
            .build();
      case STRUCT:
        return buildStructSchema((Struct) ksqlType);

      default:
        throw new KsqlException("Invalid ksql type: " + ksqlType);
    }
  }

  private static Schema buildStructSchema(final Struct struct) {
    final SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
    for (final Pair<String, Type> field: struct.getItems()) {
      structSchemaBuilder.field(field.getLeft(), getTypeSchema(field.getRight()));
    }
    return structSchemaBuilder.optional().build();
  }

  public static List<TableElement> buildTableElementsForSchema(final Schema schema) {
    return schema.fields().stream()
        .map(f -> new TableElement(f.name().toUpperCase(), getKsqlType(f.schema())))
        .collect(Collectors.toList());
  }
}
