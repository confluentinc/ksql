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

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class Statements extends AstNode {

  private final ImmutableList<Statement> statements;

  public Statements(
      final Optional<NodeLocation> location,
      final List<Statement> statements
  ) {
    super(location);
    this.statements = ImmutableList.copyOf(requireNonNull(statements, "statements"));
  }

  public List<Statement> getStatements() {
    return statements;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitStatements(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statements", statements)
        .toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Statements o = (Statements) obj;
    return Objects.equals(statements, o.statements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statements);
  }
}
