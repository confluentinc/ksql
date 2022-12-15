/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.NodeLocation;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


public class LambdaFunctionCall extends Expression {

  private final ImmutableList<String> arguments;
  private final Expression body;

  public LambdaFunctionCall(
      final List<String> name,
      final Expression body
  ) {
    this(Optional.empty(), name, body);
  }

  public LambdaFunctionCall(
      final Optional<NodeLocation> location,
      final List<String> arguments,
      final Expression body
  ) {
    super(location);
    this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments"));
    if (arguments.size() == 0) {
      throw new IllegalArgumentException(
          String.format("Lambda expression must have at least 1 argument. => %s", body.toString()));
    }
    final Set<String> set = new HashSet<>(arguments);
    if (set.size() < arguments.size()) {
      throw new IllegalArgumentException(
          String.format("Lambda arguments have duplicates: %s", arguments.toString()));
    }
    this.body = requireNonNull(body, "body is null");
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "arguments is ImmutableList")
  public List<String> getArguments() {
    return arguments;
  }

  public Expression getBody() {
    return body;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitLambdaExpression(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final LambdaFunctionCall that = (LambdaFunctionCall) obj;
    return Objects.equals(arguments, that.arguments)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(arguments, body);
  }
}
