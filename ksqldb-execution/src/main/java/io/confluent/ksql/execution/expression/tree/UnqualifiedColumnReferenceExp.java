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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression representing a column name, e.g. {@code col0} or {@code src.col1}.
 */
@Immutable
public class UnqualifiedColumnReferenceExp extends ColumnReferenceExp {

  public UnqualifiedColumnReferenceExp(final ColumnName name) {
    this(Optional.empty(), name);
  }

  public UnqualifiedColumnReferenceExp(
      final Optional<NodeLocation> location,
      final ColumnName name
  ) {
    super(location, name);
  }

  public Optional<SourceName> maybeQualifier() {
    return Optional.empty();
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitUnqualifiedColumnReference(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final UnqualifiedColumnReferenceExp that = (UnqualifiedColumnReferenceExp) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
