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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class DefineVariable extends Statement {
  private final String variableName;
  private final String variableValue;

  public DefineVariable(
      final Optional<NodeLocation> location,
      final String variableName,
      final String variableValue
  ) {
    super(location);
    this.variableName = requireNonNull(variableName, "variableName");
    this.variableValue = requireNonNull(variableValue, "variableValue");
  }

  public String getVariableName() {
    return variableName;
  }

  public String getVariableValue() {
    return variableValue;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDefineVariable(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(variableName, variableValue);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DefineVariable that = (DefineVariable) o;
    return Objects.equals(variableName, that.variableName)
        && Objects.equals(variableValue, that.variableValue);
  }

  @Override
  public String toString() {
    return "DefineVariable{"
        + "name=" + variableName
        + ", value=" + variableValue
        + '}';
  }
}
