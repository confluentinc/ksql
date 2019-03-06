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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CreateStreamAsSelect extends Statement implements CreateAsSelect {

  private final Table stream;
  private final Query query;
  private final boolean notExists;
  private final Map<String, Expression> properties;
  private final Optional<Expression> partitionByColumn;

  public CreateStreamAsSelect(
      final Table stream,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties,
      final Optional<Expression> partitionByColumn) {
    this(Optional.empty(), stream, query, notExists, properties, partitionByColumn);
  }

  public CreateStreamAsSelect(
      final Optional<NodeLocation> location,
      final Table stream,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties,
      final Optional<Expression> partitionByColumn) {
    super(location);
    this.stream = requireNonNull(stream, "streaml");
    this.query = query;
    this.notExists = notExists;
    this.properties = ImmutableMap.copyOf(
        requireNonNull(properties, "properties is null"));
    this.partitionByColumn = partitionByColumn;
  }

  @Override
  public QualifiedName getName() {
    return stream.getName();
  }

  @Override
  public Query getQuery() {
    return query;
  }

  @Override
  public Sink getSink() {
    final Map<String, Expression> sinkProperties = partitionByColumn
        .map(exp -> (Map<String, Expression>)ImmutableMap.<String, Expression>builder()
            .putAll(properties)
            .put(DdlConfig.PARTITION_BY_PROPERTY, exp)
            .build()
        )
        .orElse(properties);

    return Sink.of(stream, true, sinkProperties);
  }

  public Table getStream() {
    return stream;
  }

  public boolean isNotExists() {
    return notExists;
  }

  @Override
  public Map<String, Expression> getProperties() {
    return properties;
  }

  @Override
  public Optional<Expression> getPartitionByColumn() {
    return partitionByColumn;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateStreamAsSelect(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream, query, notExists, properties);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateStreamAsSelect o = (CreateStreamAsSelect) obj;
    return Objects.equals(stream, o.stream)
           && Objects.equals(query, o.query)
           && Objects.equals(notExists, o.notExists)
           && Objects.equals(properties, o.properties);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("stream", stream)
        .add("query", query)
        .add("notExists", notExists)
        .add("properties", properties)
        .toString();
  }
}
