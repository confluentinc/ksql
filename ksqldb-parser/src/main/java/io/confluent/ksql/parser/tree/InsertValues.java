/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Immutable
public class InsertValues extends Statement {

  private final SourceName target;
  private final ImmutableList<ColumnName> columns;
  private final ImmutableList<Expression> values;

  public InsertValues(
      final SourceName target,
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    this(Optional.empty(), target, columns, values);
  }

  public InsertValues(
      final Optional<NodeLocation> location,
      final SourceName target,
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    super(location);
    this.target = Objects.requireNonNull(target, "target");
    this.columns = ImmutableList.copyOf(Objects.requireNonNull(columns, "columns"));
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values"));

    if (values.isEmpty()) {
      throw new KsqlException("Expected some values for INSERT INTO statement.");
    }

    if (!columns.isEmpty() && columns.size() != values.size()) {
      throw new KsqlException(
          "Expected number columns and values to match: "
              + columns.stream().map(ColumnName::text).collect(Collectors.toList()).size() + ", "
              + values.size());
    }
  }

  public SourceName getTarget() {
    return target;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columns is ImmutableList")
  public List<ColumnName> getColumns() {
    return columns;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "values is ImmutableList")
  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitInsertValues(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertValues that = (InsertValues) o;
    return Objects.equals(target, that.target)
        && Objects.equals(columns, that.columns)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, columns, values);
  }

  @Override
  public String toString() {
    return "InsertValues{"
        + "target=" + target
        + ", columns=" + columns
        + ", values=<redacted>"
        + '}';
  }
}
