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

package io.confluent.ksql.planner;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ColumnExtractor;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks required columns.
 */
@Immutable
public final class RequiredColumns {

  private final ImmutableSet<ColumnReferenceExp> requiredColumns;

  public static Builder builder() {
    return new Builder(new HashSet<>());
  }

  private RequiredColumns(final Set<ColumnReferenceExp> requiredColumns) {
    this.requiredColumns = ImmutableSet
        .copyOf(requireNonNull(requiredColumns, "requiredColumns"));
  }

  public Collection<? extends ColumnReferenceExp> get() {
    return requiredColumns;
  }

  public Builder asBuilder() {
    return new Builder(new HashSet<>(requiredColumns));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RequiredColumns that = (RequiredColumns) o;
    return Objects.equals(requiredColumns, that.requiredColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(requiredColumns);
  }

  @Override
  public String toString() {
    return "RequiredColumns" + requiredColumns;
  }

  public static final class Builder {

    private final Set<ColumnReferenceExp> requiredColumns;

    public Builder(final Set<ColumnReferenceExp> requiredColumns) {
      this.requiredColumns = requireNonNull(requiredColumns, "requiredColumns");
    }

    public Builder add(final Expression expression) {
      requiredColumns.addAll(ColumnExtractor.extractColumns(expression));
      return this;
    }

    public Builder addAll(final Collection<? extends Expression> expressions) {
      expressions.forEach(this::add);
      return this;
    }

    public Builder remove(final ColumnReferenceExp expression) {
      requiredColumns.remove(expression);
      return this;
    }

    public Builder removeAll(final Collection<? extends ColumnReferenceExp> expressions) {
      expressions.forEach(this::remove);
      return this;
    }

    public RequiredColumns build() {
      return new RequiredColumns(requiredColumns);
    }
  }
}

