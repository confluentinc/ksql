/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AliasedRelation
    extends Relation {

  private final Relation relation;
  private final String alias;
  private final List<String> columnNames;

  public AliasedRelation(Relation relation, String alias, List<String> columnNames) {
    this(Optional.empty(), relation, alias, columnNames);
  }

  public AliasedRelation(NodeLocation location, Relation relation, String alias,
                         List<String> columnNames) {
    this(Optional.of(location), relation, alias, columnNames);
  }

  private AliasedRelation(Optional<NodeLocation> location, Relation relation, String alias,
                          List<String> columnNames) {
    super(location);
    requireNonNull(relation, "relation is null");
    requireNonNull(alias, " is null");

    this.relation = relation;
    this.alias = alias.toUpperCase();
    this.columnNames = columnNames;
  }

  public Relation getRelation() {
    return relation;
  }

  public String getAlias() {
    return alias;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAliasedRelation(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("relation", relation)
        .add("alias", alias)
        .add("columnNames", columnNames)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AliasedRelation that = (AliasedRelation) o;
    return Objects.equals(relation, that.relation)
           && Objects.equals(alias, that.alias)
           && Objects.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, alias, columnNames);
  }
}
