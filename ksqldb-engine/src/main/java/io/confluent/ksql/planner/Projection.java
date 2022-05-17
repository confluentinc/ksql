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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.StructAll;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds information about in a projection
 */
@Immutable
public final class Projection {

  private final boolean includesAll;
  private final ImmutableList<? extends SelectItem> selectItems;
  private final ImmutableSet<SourceName> includeAllSources;
  private final ImmutableSet<Expression> singles;
  private final ImmutableSet<Expression> structsAll;

  public static Projection of(final Collection<? extends SelectItem> selectItems) {
    return new Projection(selectItems);
  }

  private Projection(final Collection<? extends SelectItem> selects) {
    this.selectItems = ImmutableList.copyOf(selects);
    this.includesAll = selects.stream()
        .filter(Projection::isAllColumn)
        .map(AllColumns.class::cast)
        .anyMatch(si -> !si.getSource().isPresent());

    this.includeAllSources = ImmutableSet.copyOf(selects.stream()
        .filter(Projection::isAllColumn)
        .map(AllColumns.class::cast)
        .map(AllColumns::getSource)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet()));

    this.singles = ImmutableSet.copyOf(selects.stream()
        .filter(Projection::isSingleColumn)
        .map(SingleColumn.class::cast)
        .map(SingleColumn::getExpression)
        .collect(Collectors.toSet()));

    this.structsAll = ImmutableSet.copyOf(selects.stream()
        .filter(Projection::isStructAll)
        .map(StructAll.class::cast)
        .map(StructAll::getBaseStruct)
        .collect(Collectors.toSet()));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "selectItems is ImmutableList")
  public List<? extends SelectItem> selectItems() {
    return selectItems;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "singles is ImmutableSet")
  public Set<Expression> singleExpressions() {
    return singles;
  }

  /**
   * Tests is the supplied {@code expression} is contained by the projection.
   *
   * <p>The following rules are evaluated to determine if the projection contains
   * {@code expression}:
   * <ol>
   *   <li>
   *      If any of the projections items are an unqualified {@code *}, then all expressions are
   *      considered to be contained within the projection.
   *    </li>
   *    <li>
   *      If any of the projection items are a qualified {@code Something.*}, then any column
   *      reference with the same source name will be considered contained.
   *    </li>
   *    <li>
   *      If any single column within the projection matches the supplied expression, then it is
   *      contained.
   *    </li>
   *    <li>
   *      Otherwise, it is not contained.
   *    </li>
   * </ol>
   *
   * @param expression the expression to test.
   * @return {@code true} if contained, {@code false} otherwise.
   */
  public boolean containsExpression(final Expression expression) {
    if (includesAll) {
      return true;
    }

    if (expression instanceof QualifiedColumnReferenceExp) {
      final QualifiedColumnReferenceExp colRef = (QualifiedColumnReferenceExp) expression;

      if (includeAllSources.contains(colRef.getQualifier())) {
        return true;
      }
    }

    return singles.contains(expression) || structsAll.contains(expression);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Projection that = (Projection) o;
    return Objects.equals(selectItems, that.selectItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(selectItems);
  }

  private static boolean isAllColumn(final SelectItem si) {
    if (si instanceof SingleColumn) {
      return false;
    }

    if (si instanceof StructAll) {
      return false;
    }

    if (si instanceof AllColumns) {
      return true;
    }

    throw new UnsupportedOperationException("Unsupported column type: si.");
  }

  private static boolean isSingleColumn(final SelectItem si) {
    return !isAllColumn(si) && !isStructAll(si);
  }

  private static boolean isStructAll(final SelectItem si) {
    return (si instanceof StructAll);
  }
}
