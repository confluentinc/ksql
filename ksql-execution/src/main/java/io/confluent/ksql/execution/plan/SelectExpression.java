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

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.execution.expression.tree.Expression;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/**
 * Pojo holding field name and expression of a select item.
 */
@Immutable
public final class SelectExpression {
  private static final String NAME = "name";
  private static final String EXPRESSION = "expression";

  @JsonProperty(NAME)
  private final String name;
  @JsonProperty(EXPRESSION)
  private final Expression expression;

  @JsonCreator
  private SelectExpression(
      @JsonProperty(NAME) final String name,
      @JsonProperty(EXPRESSION) final Expression expression) {
    this.name = Objects.requireNonNull(name, "name");
    this.expression = Objects.requireNonNull(expression, "expression");
  }

  public static SelectExpression of(final String name, final Expression expression) {
    return new SelectExpression(name, expression);
  }

  public String getName() {
    return name;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SelectExpression that = (SelectExpression) o;
    return Objects.equals(name, that.name)
        && Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, expression);
  }

  @Override
  public String toString() {
    return "SelectExpression{"
        + "name='" + name + '\''
        + ", expression=" + expression
        + '}';
  }
}
