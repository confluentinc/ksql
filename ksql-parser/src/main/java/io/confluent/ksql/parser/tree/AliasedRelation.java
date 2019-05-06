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
public class AliasedRelation extends Relation {

  private final Relation relation;
  private final String alias;

  public AliasedRelation(
      final Relation relation,
      final String alias
  ) {
    this(Optional.empty(), relation, alias);
  }

  public AliasedRelation(
      final Optional<NodeLocation> location,
      final Relation relation,
      final String alias
  ) {
    super(location);
    this.relation = requireNonNull(relation, "relation");
    this.alias = requireNonNull(alias, "alias")
        .toUpperCase();
  }

  public Relation getRelation() {
    return relation;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAliasedRelation(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("relation", relation)
        .add("alias", alias)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AliasedRelation that = (AliasedRelation) o;
    return Objects.equals(relation, that.relation)
           && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, alias);
  }
}
