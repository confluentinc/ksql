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

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class Join extends Relation {

  private final Type type;
  private final Relation left;
  private final Relation right;
  private final JoinCriteria criteria;
  private final Optional<WithinExpression> withinExpression;

  public enum Type {
    INNER("INNER"), LEFT("LEFT OUTER"), OUTER("FULL OUTER");

    private final String formattedText;

    Type(final String formattedText) {
      this.formattedText = Objects.requireNonNull(formattedText, "formattedText");
    }

    public String getFormatted() {
      return formattedText;
    }
  }

  public Join(
      final Type type,
      final Relation left,
      final Relation right,
      final JoinCriteria criteria,
      final Optional<WithinExpression> withinExpression
  ) {
    this(Optional.empty(), type, left, right, criteria, withinExpression);
  }

  public Join(
      final Optional<NodeLocation> location,
      final Type type,
      final Relation left,
      final Relation right,
      final JoinCriteria criteria,
      final Optional<WithinExpression> withinExpression
  ) {
    super(location);
    this.type = requireNonNull(type, "type");
    this.left = requireNonNull(left, "left");
    this.right = requireNonNull(right, "right");
    this.criteria = requireNonNull(criteria, "criteria");
    this.withinExpression = requireNonNull(withinExpression, "withinExpression");
  }

  public Type getType() {
    return type;
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  public JoinCriteria getCriteria() {
    return criteria;
  }

  public Optional<WithinExpression> getWithinExpression() {
    return withinExpression;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("left", left)
        .add("right", right)
        .add("criteria", criteria)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }
    final Join join = (Join) o;
    return (type == join.type)
           && Objects.equals(left, join.left)
           && Objects.equals(right, join.right)
           && Objects.equals(criteria, join.criteria)
           && Objects.equals(withinExpression, join.withinExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right, criteria, withinExpression);
  }
}
