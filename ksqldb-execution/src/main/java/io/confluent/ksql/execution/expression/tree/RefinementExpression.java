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

import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RefinementExpression extends Expression {

  private final Optional<OutputRefinement> outputRefinement;

  public RefinementExpression(Optional<NodeLocation> location,
                              Optional<OutputRefinement> outputRefinement) {
    super(location);
    this.outputRefinement = requireNonNull(outputRefinement, "outputRefinement");
  }

  public Optional<OutputRefinement> getOutputRefinement() {
    return outputRefinement;
  }

  @Override
  protected <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitRefinementExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RefinementExpression that = (RefinementExpression) o;
    return Objects.equals(outputRefinement, that.outputRefinement);
  }

  @Override
  public int hashCode() {
    return outputRefinement.hashCode();
  }
}
