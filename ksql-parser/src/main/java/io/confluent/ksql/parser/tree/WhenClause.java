/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class WhenClause
    extends Expression {

  private final Expression operand;
  private final Expression result;

  public WhenClause(final Expression operand, final Expression result) {
    this(Optional.empty(), operand, result);
  }

  public WhenClause(
      final NodeLocation location, final Expression operand, final Expression result) {
    this(Optional.of(location), operand, result);
  }

  private WhenClause(
      final Optional<NodeLocation> location, final Expression operand, final Expression result) {
    super(location);
    this.operand = operand;
    this.result = result;
  }

  public Expression getOperand() {
    return operand;
  }

  public Expression getResult() {
    return result;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitWhenClause(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WhenClause that = (WhenClause) o;
    return Objects.equals(operand, that.operand)
           && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, result);
  }
}
