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
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.schema.ksql.LogicalSchemas.LogicalToSqlTypeConverter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class TableElement extends Node {

  private final String name;
  private final Type type;

  public TableElement(
      final String name,
      final Type type
  ) {
    this(Optional.empty(), name, type);
  }

  public TableElement(
      final Optional<NodeLocation> location,
      final String name,
      final Type type
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.type = requireNonNull(type, "type");
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitTableElement(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final TableElement o = (TableElement) obj;
    return Objects.equals(this.name, o.name)
           && Objects.equals(this.type, o.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .toString();
  }

  public static List<TableElement> fromSchema(final Schema schema) {
    final LogicalToSqlTypeConverter toSqlTypeConverter = LogicalSchemas
        .toSqlTypeConverter();

    return schema.fields().stream()
        .map(f -> new TableElement(
            f.name().toUpperCase(),
            toSqlTypeConverter.toSqlType(f.schema()))
        )
        .collect(Collectors.toList());
  }
}
