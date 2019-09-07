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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class InPredicate extends Expression {
  private static final String VALUE = "value";
  private static final String VALUE_LIST = "valueList";

  @JsonProperty(VALUE)
  private final Expression value;
  @JsonProperty(VALUE_LIST)
  private final InListExpression valueList;

  @JsonCreator
  public InPredicate(
      @JsonProperty(VALUE) final Expression value,
      @JsonProperty(VALUE_LIST) final InListExpression valueList
  ) {
    this(Optional.empty(), value, valueList);
  }

  public InPredicate(
      final Optional<NodeLocation> location,
      final Expression value,
      final InListExpression valueList
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
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitInPredicate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final InPredicate that = (InPredicate) o;
    return Objects.equals(value, that.value)
           && Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, valueList);
  }
}
