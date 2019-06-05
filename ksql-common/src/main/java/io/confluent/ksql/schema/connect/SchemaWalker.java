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
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

/**
 * Visitor pattern for connect schemas.
 */
public final class SchemaWalker {

  private static final Map<Type, BiConsumer<Visitor, Schema>> HANDLER =
      ImmutableMap.<Type, BiConsumer<Visitor, Schema>>builder()
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

  public interface Visitor {

    default void visitBoolean(Schema schema) {
    }

    default void visitNumber(Schema schema) {
    }

    default void visitInt(Schema schema) {
      visitNumber(schema);
    }

    default void visitInt8(Schema schema) {
      visitInt(schema);
    }

    default void visitInt16(Schema schema) {
      visitInt(schema);
    }

    default void visitInt32(Schema schema) {
      visitInt(schema);
    }

    default void visitInt64(Schema schema) {
      visitInt(schema);
    }

    default void visitFloat(Schema schema) {
      visitNumber(schema);
    }

    default void visitFloat32(Schema schema) {
      visitFloat(schema);
    }

    default void visitFloat64(Schema schema) {
      visitFloat(schema);
    }

    default void visitString(Schema schema) {
    }

    default void visitBytes(Schema schema) {
    }

    /**
     * @return {@code true} to visit element schema.
     */
    default boolean visitArray(Schema schema) {
      return true;
    }

    /**
     * @return {@code true} to visit key and value schemas.
     */
    default boolean visitMap(Schema schema) {
      return true;
    }

    /**
     * @return {@code true} to visit field schemas.
     */
    default boolean visitStruct(Schema schema) {
      return true;
    }
  }

  public static void visit(final Schema schema, final Visitor visitor) {
    final BiConsumer<Visitor, Schema> handler = HANDLER.get(schema.type());
    if (handler == null) {
      throw new UnsupportedOperationException("Unsupported schema type: " + schema.type());
    }

    handler.accept(visitor, schema);
  }

  private static void visitArray(final Visitor visitor, final Schema schema) {
    if (visitor.visitArray(schema)) {
      visit(schema.valueSchema(), visitor);
    }
  }

  private static void visitMap(final Visitor visitor, final Schema schema) {
    if (visitor.visitMap(schema)) {
      visit(schema.keySchema(), visitor);
      visit(schema.valueSchema(), visitor);
    }
  }

  private static void visitStruct(final Visitor visitor, final Schema schema) {
    if (visitor.visitStruct(schema)) {
      schema.fields().forEach(f -> visit(f.schema(), visitor));
    }
  }
}
