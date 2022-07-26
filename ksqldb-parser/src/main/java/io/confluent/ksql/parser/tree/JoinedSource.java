/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class JoinedSource extends Relation {

  private final Relation relation;
  private final Type type;
  private final JoinCriteria criteria;
  private final Optional<WithinExpression> withinExpression;

  public JoinedSource(
      final Optional<NodeLocation> location,
      final Relation relation,
      final Type type,
      final JoinCriteria criteria,
      final Optional<WithinExpression> withinExpression
  ) {
    super(location);
    this.relation = Objects.requireNonNull(relation, "relation");
    this.type = Objects.requireNonNull(type, "type");
    this.criteria = Objects.requireNonNull(criteria, "criteria");
    this.withinExpression = Objects.requireNonNull(withinExpression, "withinExpression");
  }

  public Relation getRelation() {
    return relation;
  }

  public Type getType() {
    return type;
  }

  public JoinCriteria getCriteria() {
    return criteria;
  }

  public Optional<WithinExpression> getWithinExpression() {
    return withinExpression;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitJoinedSource(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JoinedSource that = (JoinedSource) o;
    return Objects.equals(relation, that.relation)
        && Objects.equals(type, that.type)
        && Objects.equals(criteria, that.criteria)
        && Objects.equals(withinExpression, that.withinExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, type, criteria, withinExpression);
  }

  @Override
  public String toString() {
    return "JoinedSource{"
        + "relation=" + relation
        + ", type=" + type
        + ", criteria=" + criteria
        + ", withinExpression=" + withinExpression
        + '}';
  }

  public enum Type {
    INNER("INNER"), LEFT("LEFT OUTER"), RIGHT("RIGHT OUTER"), OUTER("FULL OUTER");

    private final String formattedText;

    Type(final String formattedText) {
      this.formattedText = Objects.requireNonNull(formattedText, "formattedText");
    }

    public String getFormatted() {
      return formattedText;
    }
  }
}