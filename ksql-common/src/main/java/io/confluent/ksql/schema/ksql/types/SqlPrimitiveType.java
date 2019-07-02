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

package io.confluent.ksql.schema.ksql.types;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

@Immutable
public final class SqlPrimitiveType extends SqlType {

  private static final ImmutableMap<SqlBaseType, SqlPrimitiveType> TYPES =
      ImmutableMap.<SqlBaseType, SqlPrimitiveType>builder()
          .put(SqlBaseType.BOOLEAN, new SqlPrimitiveType(SqlBaseType.BOOLEAN))
          .put(SqlBaseType.INTEGER, new SqlPrimitiveType(SqlBaseType.INTEGER))
          .put(SqlBaseType.BIGINT, new SqlPrimitiveType(SqlBaseType.BIGINT))
          .put(SqlBaseType.DOUBLE, new SqlPrimitiveType(SqlBaseType.DOUBLE))
          .put(SqlBaseType.STRING, new SqlPrimitiveType(SqlBaseType.STRING))
          .build();

  public static SqlPrimitiveType of(final String typeName) {
    switch (typeName.toUpperCase()) {
      case "INT":
        return SqlPrimitiveType.of(SqlBaseType.INTEGER);
      case "VARCHAR":
        return SqlPrimitiveType.of(SqlBaseType.STRING);
      default:
        try {
          final SqlBaseType sqlType = SqlBaseType.valueOf(typeName.toUpperCase());
          return SqlPrimitiveType.of(sqlType);
        } catch (final IllegalArgumentException e) {
          throw new KsqlException("Unknown primitive type: " + typeName, e);
        }
    }
  }

  public static SqlPrimitiveType of(final SqlBaseType sqlType) {
    final SqlPrimitiveType primitive = TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (primitive == null) {
      throw new KsqlException("Invalid primitive type: " + sqlType);
    }
    return primitive;
  }

  private SqlPrimitiveType(final SqlBaseType baseType) {
    super(baseType);
  }

  @Override
  public boolean supportsCast() {
    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqlPrimitiveType)) {
      return false;
    }

    final SqlPrimitiveType that = (SqlPrimitiveType) o;
    return Objects.equals(this.baseType(), that.baseType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseType());
  }

  @Override
  public String toString() {
    return baseType().toString();
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return toString();
  }
}
