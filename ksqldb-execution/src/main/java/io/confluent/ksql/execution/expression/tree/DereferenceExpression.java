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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression representing a STRUCT field, e.g. {@code Source.Column->fieldName}.
 */
@Immutable
public class DereferenceExpression extends Expression {

  private final Expression base;
  private final String fieldName;

  /**
   * @param location  the location in the source SQL.
   * @param base      the base expression that resolves to a struct.
   * @param fieldName the name of the field within the struct.
   */
  public DereferenceExpression(
      final Optional<NodeLocation> location,
      final Expression base,
      final String fieldName
  ) {
    super(location);
    this.base = requireNonNull(base, "base");
    this.fieldName = requireNonNull(fieldName, "fieldName");
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitDereferenceExpression(this, context);
  }

  public Expression getBase() {
    return base;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DereferenceExpression that = (DereferenceExpression) o;
    return Objects.equals(base, that.base)
        && Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, fieldName);
  }
}
