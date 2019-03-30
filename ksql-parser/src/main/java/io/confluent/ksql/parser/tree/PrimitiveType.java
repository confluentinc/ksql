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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

@Immutable
public final class PrimitiveType extends Type {

  /* List of parameters for the type (i.e. DECIMAL(6,2)). */
  final ImmutableList<Integer> typeParameters;

  private static final ImmutableMap<SqlType, Function<ImmutableList<Integer>, PrimitiveType>>
      TYPES = ImmutableMap.<SqlType, Function<ImmutableList<Integer>, PrimitiveType>>builder()
      .put(SqlType.BOOLEAN, p -> new PrimitiveType(SqlType.BOOLEAN))
      .put(SqlType.INTEGER, p -> new PrimitiveType(SqlType.INTEGER))
      .put(SqlType.BIGINT,  p -> new PrimitiveType(SqlType.BIGINT))
      .put(SqlType.DOUBLE,  p -> new PrimitiveType(SqlType.DOUBLE))
      .put(SqlType.STRING,  p -> new PrimitiveType(SqlType.STRING))
      .put(SqlType.DECIMAL, p -> new PrimitiveType(SqlType.DECIMAL, p))
      .build();

  public static PrimitiveType of(final String typeName) {
    return of(typeName, ImmutableList.of());
  }

  public static PrimitiveType of(
      final String typeName,
      @Nullable final List<Integer> typeParameters
  ) {
    switch (typeName.toUpperCase()) {
      case "INT":
        return PrimitiveType.of(SqlType.INTEGER);
      case "VARCHAR":
        return PrimitiveType.of(SqlType.STRING);
      case "DEC": // SQL standard states that DEC is similar to DECIMAL
        return PrimitiveType.of(SqlType.DECIMAL, typeParameters);
      default:
        try {
          final SqlType sqlType = SqlType.valueOf(typeName.toUpperCase());
          return PrimitiveType.of(sqlType, typeParameters);
        } catch (final IllegalArgumentException e) {
          throw new KsqlException("Unknown primitive type: " + typeName, e);
        }
    }
  }

  public static PrimitiveType of(final SqlType sqlType) {
    return function(sqlType).apply(null);
  }

  public static PrimitiveType of(
      final SqlType sqlType,
      @Nullable final List<Integer> sqlTypeParameters
  ) {
    if (sqlTypeParameters == null) {
      return function(sqlType).apply(ImmutableList.of());
    } else {
      return function(sqlType).apply(ImmutableList.copyOf(sqlTypeParameters));
    }
  }

  private static Function<ImmutableList<Integer>, PrimitiveType> function(final SqlType sqlType) {
    final Function<ImmutableList<Integer>, PrimitiveType> function =
        TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (function == null) {
      throw new KsqlException("Invalid primitive type: " + sqlType);
    }

    return function;
  }

  private PrimitiveType(final SqlType sqlType) {
    this(sqlType, ImmutableList.of());
  }

  private PrimitiveType(final SqlType sqlType, final ImmutableList<Integer> typeParameters) {
    super(Optional.empty(), sqlType);
    this.typeParameters = typeParameters;

    // Verify this type accepts enough parameters, otherwise throw an exception
    checkTypeParameters();
  }

  public ImmutableList<Integer> getSqlTypeParameters() {
    return typeParameters;
  }

  private void checkTypeParameters() {
    switch (getSqlType()) {
      case DECIMAL:
        if (typeParameters.size() != 2) {
          throw new KsqlException(
              "Primitive type requires 2 parameters: " + getSqlType());
        }

        break;
      default:
        if (typeParameters.size() > 0) {
          throw new KsqlException(
              "Primitive type does not support parameters: " + getSqlType());
        }
    }
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPrimitiveType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }

    if (this == o) {
      return true;
    }

    if (!(o instanceof PrimitiveType)) {
      return false;
    }

    final PrimitiveType that = (PrimitiveType) o;
    return Objects.equals(this.getSqlType(), that.getSqlType())
        && Objects.equals(this.getSqlTypeParameters(), that.getSqlTypeParameters());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSqlType(), typeParameters);
  }
}
