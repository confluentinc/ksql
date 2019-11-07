/*
 * Copyright 2018 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class InPredicate extends Expression {

  private final Expression value;
  private final InListExpression valueList;

  public InPredicate(Expression value, InListExpression valueList) {
    this(Optional.empty(), value, valueList);
  }

  public InPredicate(
      Optional<NodeLocation> location, Expression value, InListExpression valueList
  ) {
    super(location);
    this.value = requireNonNull(value, "value");
    this.valueList = requireNonNull(valueList, "valueList");
  }

  public Expression getValue() {
    return value;
  }

  public InListExpression getValueList() {
    return valueList;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitInPredicate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InPredicate that = (InPredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, valueList);
  }
}
