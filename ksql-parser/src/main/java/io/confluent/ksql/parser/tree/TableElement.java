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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * An element in the schema of a table or stream.
 */
@Immutable
public final class TableElement extends Node {

  private final String name;
  private final Type type;

  /**
   * @param name the name of the element.
   * @param type the sql type of the element.
   */
  public TableElement(
      final String name,
      final Type type
  ) {
    this(Optional.empty(), name, type);
  }

  /**
   * @param location the location in the SQL text.
   * @param name the name of the element.
   * @param type the sql type of the element.
   */
  public TableElement(
      final Optional<NodeLocation> location,
      final String name,
      final Type type
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.type = requireNonNull(type, "type");

    validate();
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
    return "TableElement{"
        + "name='" + name + '\''
        + ", type=" + type
        + '}';
  }

  private void validate() {
    if (name.toUpperCase().equals(SchemaUtil.ROWTIME_NAME)
        || name.toUpperCase().equals(SchemaUtil.ROWKEY_NAME)) {
      throw new ParsingException("'" + name + "' is a reserved field name.", getLocation());
    }
  }
}
