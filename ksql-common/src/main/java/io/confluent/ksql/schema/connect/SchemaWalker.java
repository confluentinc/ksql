/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.connect;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Visitor pattern for connect schemas.
 */
public final class SchemaWalker {

  private static final Map<Type, BiFunction<Visitor<?>, Schema, Object>> HANDLER =
      ImmutableMap.<Type, BiFunction<Visitor<?>, Schema, Object>>builder()
          .put(Type.BOOLEAN, Visitor::visitBoolean)
          .put(Type.INT8, Visitor::visitInt8)
          .put(Type.INT16, Visitor::visitInt16)
          .put(Type.INT32, Visitor::visitInt32)
          .put(Type.INT64, Visitor::visitInt64)
          .put(Type.FLOAT32, Visitor::visitFloat32)
          .put(Type.FLOAT64, Visitor::visitFloat64)
          .put(Type.STRING, Visitor::visitString)
          .put(Type.BYTES, Visitor::visitBytes)
          .put(Type.ARRAY, SchemaWalker::visitArray)
          .put(Type.MAP, SchemaWalker::visitMap)
          .put(Type.STRUCT, SchemaWalker::visitStruct)
          .build();

  private SchemaWalker() {
  }

  public interface Visitor<T> {

    default T visitSchema(Schema schema) {
      throw new UnsupportedOperationException("Unsupported schema type: " + schema);
    }

    default T visitPrimitive(Schema schema) {
      return visitSchema(schema);
    }

    default T visitBoolean(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitInt8(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitInt16(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitInt32(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitInt64(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitFloat32(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitFloat64(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitString(Schema schema) {
      return visitPrimitive(schema);
    }

    default T visitBytes(Schema schema) {
      return visitSchema(schema);
    }

    default T visitArray(Schema schema, T element) {
      return visitSchema(schema);
    }

    default T visitMap(Schema schema, T key, T value) {
      return visitSchema(schema);
    }

    default T visitStruct(Schema schema, List<? extends T> fields) {
      return visitSchema(schema);
    }

    default T visitField(Field field, T type) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T visit(final Schema schema, final Visitor<T> visitor) {
    final BiFunction<Visitor<?>, Schema, Object> handler = HANDLER.get(schema.type());
    if (handler == null) {
      throw new UnsupportedOperationException("Unsupported schema type: " + schema.type());
    }

    return (T) handler.apply(visitor, schema);
  }

  private static <T> T visitArray(final Visitor<T> visitor, final Schema schema) {
    final T element = visit(schema.valueSchema(), visitor);
    return visitor.visitArray(schema, element);
  }

  private static <T> T visitMap(final Visitor<T> visitor, final Schema schema) {
    final T key = visit(schema.keySchema(), visitor);
    final T value = visit(schema.valueSchema(), visitor);
    return visitor.visitMap(schema, key, value);
  }

  private static <T> T visitStruct(final Visitor<T> visitor, final Schema schema) {
    final List<T> fields = schema.fields().stream()
        .map(field -> visitor.visitField(
            field,
            SchemaWalker.<T>visit(field.schema(), visitor))
        )
        .collect(Collectors.toList());

    return visitor.visitStruct(schema, fields);
  }
}
