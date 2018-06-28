/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DereferenceExpression
    extends Expression {

  private final Expression base;
  private final String fieldName;

  public DereferenceExpression(Expression base, String fieldName) {
    this(Optional.empty(), base, fieldName);
  }

  public DereferenceExpression(NodeLocation location, Expression base, String fieldName) {
    this(Optional.of(location), base, fieldName);
  }

  public DereferenceExpression(Optional<NodeLocation> location, Expression base,
                                String fieldName) {
    super(location);
    checkArgument(base != null, "base is null");
    checkArgument(fieldName != null, "fieldName is null");
    this.base = base;
    this.fieldName = fieldName;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDereferenceExpression(this, context);
  }

  public Expression getBase() {
    return base;
  }

  public String getFieldName() {
    return fieldName;
  }

  /**
   * If this DereferenceExpression looks like a QualifiedName, return QualifiedName.
   * Otherwise return null
   */
  public static QualifiedName getQualifiedName(DereferenceExpression expression) {
    List<String> parts = tryParseParts(expression.base, expression.fieldName);
    return parts == null ? null : QualifiedName.of(parts);
  }

  private static List<String> tryParseParts(Expression base, String fieldName) {
    if (base instanceof QualifiedNameReference) {
      List<String> newList = new ArrayList<>(((QualifiedNameReference) base).getName().getParts());
      newList.add(fieldName);
      return newList;
    } else if (base instanceof DereferenceExpression) {
      QualifiedName baseQualifiedName = getQualifiedName((DereferenceExpression) base);
      if (baseQualifiedName != null) {
        List<String> newList = new ArrayList<>(baseQualifiedName.getParts());
        newList.add(fieldName);
        return newList;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DereferenceExpression that = (DereferenceExpression) o;
    return Objects.equals(base, that.base)
           && Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, fieldName);
  }
}
