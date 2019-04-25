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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class CreateAsSelect extends Statement implements QueryContainer {

  private final QualifiedName name;
  private final Query query;
  private final boolean notExists;
  private final ImmutableMap<String, Expression> properties;
  private final Optional<Expression> partitionByColumn;

  CreateAsSelect(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties,
      final Optional<Expression> partitionByColumn) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.query = requireNonNull(query, "query is null");
    this.notExists = notExists;
    this.properties = ImmutableMap
        .copyOf(requireNonNull(properties, "properties is null"));
    this.partitionByColumn = requireNonNull(partitionByColumn, "partitionByColumn");
  }

  CreateAsSelect(
      final CreateAsSelect other,
      final Map<String, Expression> properties) {
    this(
        other.getLocation(),
        other.name,
        other.query,
        other.notExists,
        properties,
        other.partitionByColumn);
  }

  public abstract CreateAsSelect copyWith(Map<String, Expression> properties);

  public QualifiedName getName() {
    return name;
  }

  @Override
  public Query getQuery() {
    return query;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  public Optional<Expression> getPartitionByColumn() {
    return partitionByColumn;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, properties, notExists, getClass());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateAsSelect o = (CreateAsSelect) obj;
    return Objects.equals(name, o.name)
        && Objects.equals(query, o.query)
        && Objects.equals(notExists, o.notExists)
        && Objects.equals(properties, o.properties)
        && Objects.equals(partitionByColumn, o.partitionByColumn);
  }

  @Override
  public String toString() {
    return "CreateAsSelect{" + "name=" + name
        + ", query=" + query
        + ", notExists=" + notExists
        + ", properties=" + properties
        + ", partitionByColumn=" + partitionByColumn
        + '}';
  }
}
