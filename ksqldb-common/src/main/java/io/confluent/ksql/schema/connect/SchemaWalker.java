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

  private static final Map<Type, BiFunction<Visitor<?, ?>, Schema, Object>> HANDLER =
      ImmutableMap.<Type, BiFunction<Visitor<?, ?>, Schema, Object>>builder()
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

  public interface Visitor<S, F> {

    default S visitSchema(final Schema schema) {
      throw new UnsupportedOperationException("Unsupported schema type: " + schema);
    }

    default S visitPrimitive(final Schema schema) {
      return visitSchema(schema);
    }

    default S visitBoolean(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitInt8(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitInt16(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitInt32(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitInt64(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitFloat32(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitFloat64(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitString(final Schema schema) {
      return visitPrimitive(schema);
    }

    default S visitBytes(final Schema schema) {
      return visitSchema(schema);
    }

    default S visitArray(final Schema schema, final S element) {
      return visitSchema(schema);
    }

    default S visitMap(final Schema schema, final S key, final S value) {
      return visitSchema(schema);
    }

    default S visitStruct(final Schema schema, final List<? extends F> fields) {
      return visitSchema(schema);
    }

    default F visitField(final Field field, final S type) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <S, F> S visit(final Schema schema, final Visitor<S, F> visitor) {
    final BiFunction<Visitor<?, ?>, Schema, Object> handler = HANDLER.get(schema.type());
    if (handler == null) {
      throw new UnsupportedOperationException("Unsupported schema type: " + schema.type());
    }

    return (S) handler.apply(visitor, schema);
  }

  private static <S, F> S visitArray(final Visitor<S, F> visitor, final Schema schema) {
    final S element = visit(schema.valueSchema(), visitor);
    return visitor.visitArray(schema, element);
  }

  private static <S, F> S visitMap(final Visitor<S, F> visitor, final Schema schema) {
    final S key = visit(schema.keySchema(), visitor);
    final S value = visit(schema.valueSchema(), visitor);
    return visitor.visitMap(schema, key, value);
  }

  private static <S, F> S visitStruct(final Visitor<S, F> visitor, final Schema schema) {
    final List<F> fields = schema.fields().stream()
        .map(field -> visitor.visitField(
            field,
            SchemaWalker.<S, F>visit(field.schema(), visitor))
        )
        .collect(Collectors.toList());

    return visitor.visitStruct(schema, fields);
  }
}
