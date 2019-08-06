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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class Type extends Expression {

  private final SqlType sqlType;

  public Type(final SqlType sqlType) {
    this(Optional.empty(), sqlType);
  }

  public Type(final Optional<NodeLocation> location, final SqlType sqlType) {
    super(location);
    this.sqlType = requireNonNull(sqlType, "sqlType");
  }

  public SqlType getSqlType() {
    return sqlType;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Type type = (Type) o;
    return Objects.equals(sqlType, type.sqlType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sqlType);
  }
}
