/*
 * Copyright 2020 Confluent Inc.
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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class AlterOption extends AstNode {
  private final String columnName;
  private final Type type;

  public AlterOption(final String columnName, final Type type) {
    this(Optional.empty(), columnName, type);
  }

  public AlterOption(
      final Optional<NodeLocation> location,
      final String columnName,
      final Type type
  ) {
    super(location);
    this.columnName = columnName;
    this.type = type;
  }

  public String getColumnName() {
    return columnName;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "AlterOption{"
        + "columnName='" + columnName
        + '\'' + ", type=" + type
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterOption that = (AlterOption) o;
    return Objects.equals(columnName, that.columnName)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, type);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterOption(this, context);
  }
}
