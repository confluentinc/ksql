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

package io.confluent.ksql.execution.expression.tree;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.util.Objects;
import java.util.Optional;

public class QualifiedColumnReferenceExp extends ColumnReferenceExp {
  private final SourceName qualifier;

  public QualifiedColumnReferenceExp(final SourceName qualifier, final ColumnRef name) {
    this(Optional.empty(), qualifier, name);
  }

  public QualifiedColumnReferenceExp(
      final Optional<NodeLocation> location,
      final SourceName qualifier,
      final ColumnRef name
  ) {
    super(location, name);
    this.qualifier = Objects.requireNonNull(qualifier);
  }

  public SourceName getQualifier() {
    return qualifier;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitQualifiedColumnReference(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QualifiedColumnReferenceExp that = (QualifiedColumnReferenceExp) o;
    return Objects.equals(qualifier, that.qualifier)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {

    return Objects.hash(qualifier, name);
  }
}
