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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

public class PrimitiveType extends Type {

  final SqlType sqlType;

  public PrimitiveType(final SqlType sqlType) {
    this(Optional.empty(), sqlType);
  }

  public PrimitiveType(final NodeLocation location, final SqlType sqlType) {
    this(Optional.of(location), sqlType);
  }

  private PrimitiveType(final Optional<NodeLocation> location, final SqlType sqlType) {
    super(location, sqlType);
    requireNonNull(sqlType, "sqlType is null");
    this.sqlType = sqlType;
  }

  public static PrimitiveType getPrimitiveType(final String typeName) {
    switch (typeName) {
      case "BOOLEAN":
        return new PrimitiveType(SqlType.BOOLEAN);
      case "INT":
      case "INTEGER":
        return new PrimitiveType(SqlType.INTEGER);
      case "BIGINT":
        return new PrimitiveType(SqlType.BIGINT);
      case "DOUBLE":
        return new PrimitiveType(SqlType.DOUBLE);
      case "VARCHAR":
      case "STRING":
        return new PrimitiveType(SqlType.STRING);
      default:
        throw new KsqlException("Invalid primitive column type: " + typeName);
    }
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPrimitiveType(this, context);
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sqlType);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof PrimitiveType) {
      return ((PrimitiveType) obj).getSqlType() == sqlType;
    }
    return false;
  }
}
