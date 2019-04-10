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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class PrimitiveType extends Type {

  private static final ImmutableMap<SqlType, PrimitiveType> TYPES =
      ImmutableMap.<SqlType, PrimitiveType>builder()
      .put(SqlType.BOOLEAN, new PrimitiveType(SqlType.BOOLEAN))
      .put(SqlType.INTEGER, new PrimitiveType(SqlType.INTEGER))
      .put(SqlType.BIGINT,  new PrimitiveType(SqlType.BIGINT))
      .put(SqlType.DOUBLE,  new PrimitiveType(SqlType.DOUBLE))
      .put(SqlType.STRING,  new PrimitiveType(SqlType.STRING))
      .build();


  public static PrimitiveType of(final String typeName) {
    switch (typeName.toUpperCase()) {
      case "INT":
        return PrimitiveType.of(SqlType.INTEGER);
      case "VARCHAR":
        return PrimitiveType.of(SqlType.STRING);
      default:
        try {
          final SqlType sqlType = SqlType.valueOf(typeName.toUpperCase());
          return PrimitiveType.of(sqlType);
        } catch (final IllegalArgumentException e) {
          throw new KsqlException("Unknown primitive type: " + typeName, e);
        }
    }
  }

  public static PrimitiveType of(final SqlType sqlType) {
    final PrimitiveType primitive = TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (primitive == null) {
      throw new KsqlException("Invalid primitive type: " + sqlType);
    }
    return primitive;
  }

  private PrimitiveType(final SqlType sqlType) {
    super(Optional.empty(), sqlType);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPrimitiveType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PrimitiveType)) {
      return false;
    }

    final PrimitiveType that = (PrimitiveType) o;
    return Objects.equals(this.getSqlType(), that.getSqlType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSqlType());
  }
}
