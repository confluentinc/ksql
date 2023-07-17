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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class PartitionBy extends AstNode {

  private final ImmutableList<Expression> partitionByExpressions;

  public PartitionBy(
      final Optional<NodeLocation> location,
      final List<Expression> partitionByExpressions
  ) {
    super(location);
    this.partitionByExpressions = ImmutableList
        .copyOf(requireNonNull(partitionByExpressions, "partitionByExpressions"));

    if (partitionByExpressions.isEmpty()) {
      throw new KsqlException("PARTITION BY requires at least one expression");
    }

    final HashSet<Object> partitionBys = new HashSet<>(partitionByExpressions.size());
    partitionByExpressions.forEach(exp -> {
      if (!partitionBys.add(exp)) {
        throw new KsqlException("Duplicate PARTITION BY expression: " + exp);
      }
    });
  }

  public List<Expression> getExpressions() {
    return partitionByExpressions;
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPartitionBy(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PartitionBy partitionBy = (PartitionBy) o;
    return Objects.equals(partitionByExpressions, partitionBy.partitionByExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionByExpressions);
  }

  @Override
  public String toString() {
    return "PartitionBy{"
        + "partitionByExpressions=" + partitionByExpressions
        + '}';
  }
}
