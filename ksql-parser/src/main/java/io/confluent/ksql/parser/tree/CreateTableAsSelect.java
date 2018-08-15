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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CreateTableAsSelect extends Statement implements CreateAsSelect {

  private final QualifiedName name;
  private final Query query;
  private final boolean notExists;
  private final Map<String, Expression> properties;

  public CreateTableAsSelect(
      final QualifiedName name,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties
  ) {
    this(Optional.empty(), name, query, notExists, properties);
  }

  public CreateTableAsSelect(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties
  ) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.query = requireNonNull(query, "query is null");
    this.notExists = notExists;
    this.properties = ImmutableMap
        .copyOf(requireNonNull(properties, "properties is null"));
  }

  public QualifiedName getName() {
    return name;
  }

  public Query getQuery() {
    return query;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  @Override
  public Optional<Expression> getPartitionByColumn() {
    return Optional.empty();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTableAsSelect(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, properties);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateTableAsSelect o = (CreateTableAsSelect) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(query, o.query)
           && Objects.equals(notExists, o.notExists)
           && Objects.equals(properties, o.properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("query", query)
        .add("notExists", notExists)
        .add("properties", properties)
        .toString();
  }
}
