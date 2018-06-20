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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class InsertInto
    extends Statement {
  private final QualifiedName target;
  private final Query query;
  private final Optional<Expression> partitionByColumn;

  public InsertInto(
      QualifiedName target,
      Query query, Optional<Expression> partitionByColumn
  ) {
    this(Optional.empty(), target, query, partitionByColumn);
  }

  public InsertInto(
      Optional<NodeLocation> location,
      QualifiedName target,
      Query query,
      Optional<Expression> partitionByColumn) {
    super(location);
    this.target = requireNonNull(target, "target is null");
    this.query = requireNonNull(query, "query is null");
    this.partitionByColumn = partitionByColumn;
  }

  public QualifiedName getTarget() {
    return target;
  }

  public Query getQuery() {
    return query;
  }

  public Optional<Expression> getPartitionByColumn() {
    return partitionByColumn;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsertInto(this, context);
  }


  @Override
  public int hashCode() {
    return Objects.hash(target, query);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    InsertInto o = (InsertInto) obj;
    return Objects.equals(target, o.target)
           && Objects.equals(query, o.query)
           && Objects.equals(partitionByColumn, o.partitionByColumn);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("target", target)
        .add("query", query)
        .toString();
  }
}

