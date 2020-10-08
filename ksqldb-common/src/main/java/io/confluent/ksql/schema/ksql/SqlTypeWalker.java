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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Visitor pattern for ksql types.
 */
public final class SqlTypeWalker {

  private static final Map<SqlBaseType, BiFunction<SqlTypeWalker.Visitor<?, ?>, SqlType, Object>>
      HANDLER = ImmutableMap.<SqlBaseType, BiFunction<SqlTypeWalker.Visitor<?, ?>, SqlType, Object>>
      builder()
      .put(SqlBaseType.BOOLEAN, (v, t) -> v.visitBoolean((SqlPrimitiveType) t))
      .put(SqlBaseType.INTEGER, (v, t) -> v.visitInt((SqlPrimitiveType) t))
      .put(SqlBaseType.BIGINT, (v, t) -> v.visitBigInt((SqlPrimitiveType) t))
      .put(SqlBaseType.DOUBLE, (v, t) -> v.visitDouble((SqlPrimitiveType) t))
      .put(SqlBaseType.STRING, (v, t) -> v.visitString((SqlPrimitiveType) t))
      .put(SqlBaseType.DECIMAL, (v, t) -> v.visitDecimal((SqlDecimal) t))
      .put(SqlBaseType.ARRAY, SqlTypeWalker::visitArray)
      .put(SqlBaseType.MAP, SqlTypeWalker::visitMap)
      .put(SqlBaseType.STRUCT, SqlTypeWalker::visitStruct)
      .build();

  private SqlTypeWalker() {
  }

  public interface Visitor<S, F> {

    default S visitType(final SqlType schema) {
      throw new UnsupportedOperationException("Unsupported sql type: " + schema);
    }

    default S visitPrimitive(final SqlPrimitiveType type) {
      return visitType(type);
    }

    default S visitBoolean(final SqlPrimitiveType type) {
      return visitPrimitive(type);
    }

    default S visitInt(final SqlPrimitiveType type) {
      return visitPrimitive(type);
    }

    default S visitBigInt(final SqlPrimitiveType type) {
      return visitPrimitive(type);
    }

    default S visitDouble(final SqlPrimitiveType type) {
      return visitPrimitive(type);
    }

    default S visitString(final SqlPrimitiveType type) {
      return visitPrimitive(type);
    }

    default S visitDecimal(final SqlDecimal type) {
      return visitType(type);
    }

    default S visitArray(final SqlArray type, final S element) {
      return visitType(type);
    }

    default S visitMap(final SqlMap type, final S key, final S value) {
      return visitType(type);
    }

    default S visitStruct(final SqlStruct type, final List<? extends F> fields) {
      return visitType(type);
    }

    default F visitField(final Field field, final S type) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <S, F> S visit(final SqlType type, final SqlTypeWalker.Visitor<S, F> visitor) {
    final BiFunction<SqlTypeWalker.Visitor<?, ?>, SqlType, Object> handler = HANDLER
        .get(type.baseType());

    if (handler == null) {
      throw new UnsupportedOperationException("Unsupported schema type: " + type.baseType());
    }

    return (S) handler.apply(visitor, type);
  }

  public static <S, F> F visit(final Field field, final SqlTypeWalker.Visitor<S, F> visitor) {
    return visitField(visitor, field);
  }

  private static <S, F> S visitArray(
      final SqlTypeWalker.Visitor<S, F> visitor,
      final SqlType type
  ) {
    final SqlArray array = (SqlArray) type;
    final S element = visit(array.getItemType(), visitor);
    return visitor.visitArray(array, element);
  }

  private static <S, F> S visitMap(
      final SqlTypeWalker.Visitor<S, F> visitor,
      final SqlType type
  ) {
    final SqlMap map = (SqlMap) type;
    final S key = visit(map.getKeyType(), visitor);
    final S value = visit(map.getValueType(), visitor);
    return visitor.visitMap(map, key, value);
  }

  private static <S, F> S visitStruct(
      final SqlTypeWalker.Visitor<S, F> visitor,
      final SqlType type
  ) {
    final SqlStruct struct = (SqlStruct) type;
    final List<F> fields = struct.fields().stream()
        .map(field -> visitField(visitor, field))
        .collect(Collectors.toList());

    return visitor.visitStruct(struct, fields);
  }

  private static <S, F> F visitField(final Visitor<S, F> visitor, final Field field) {
    final S fieldType = SqlTypeWalker.visit(field.type(), visitor);
    return visitor.visitField(field, fieldType);
  }
}
