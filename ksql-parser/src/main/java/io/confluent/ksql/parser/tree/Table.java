/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Table
    extends QueryBody {

  private final boolean isStdOut;
  private final Map<String, Expression> properties  = new HashMap<>();
  private final QualifiedName name;

  public Table(final QualifiedName name) {
    this(Optional.empty(), name, false);
  }

  public Table(final QualifiedName name,final  boolean isStdOut) {
    this(Optional.empty(), name, isStdOut);
  }

  public Table(final NodeLocation location, final QualifiedName name) {
    this(Optional.of(location), name, false);
  }

  public Table(final NodeLocation location, final QualifiedName name, final boolean isStdOut) {
    this(Optional.of(location), name, isStdOut);
  }

  public Table(
      final NodeLocation location,
      final QualifiedName name,
      final boolean isStdOut,
      final Map<String, Expression> properties) {
    this(Optional.ofNullable(location), name, isStdOut);
    this.setProperties(properties);
  }

  public Table(
      final QualifiedName name,
      final boolean isStdOut,
      final  Map<String, Expression> properties) {
    this(Optional.empty(), name, isStdOut);
    this.setProperties(properties);
  }

  private Table(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final boolean isStdOut) {
    super(location);
    this.name = name;
    this.isStdOut = isStdOut;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isStdOut() {
    return isStdOut;
  }

  public Map<String, Expression> getProperties() {
    return properties;
  }

  public void setProperties(
      final Map<String, Expression> properties) {
    this.properties.clear();
    this.properties.putAll(properties);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitTable(this, context);
  }

  @Override
  public String toString() {
    return name.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Table table = (Table) o;
    return Objects.equals(name, table.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
