/*
 * Copyright 2021 Confluent Inc.
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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class LambdaVariable extends Expression {

  private final String lambdaCharacter;

  public LambdaVariable(final String lambdaCharacter) {
    this(Optional.empty(), lambdaCharacter);
  }

  public LambdaVariable(final Optional<NodeLocation> location, final String lambdaCharacter) {
    super(location);
    this.lambdaCharacter = lambdaCharacter;
  }

  public String getLambdaCharacter() {
    return lambdaCharacter;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitLambdaVariable(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LambdaVariable that = (LambdaVariable) o;
    return lambdaCharacter.equals(that.lambdaCharacter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(lambdaCharacter);
  }
}
