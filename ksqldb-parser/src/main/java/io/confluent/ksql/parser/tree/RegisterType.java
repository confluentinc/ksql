/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public class RegisterType extends Statement implements ExecutableDdlStatement {

  private final Type type;
  private final String name;
  private final boolean ifNotExists;

  public RegisterType(
      final Optional<NodeLocation> location,
      final String name, final Type type,
      final boolean ifNotExists
  ) {
    super(location);
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
    this.ifNotExists = Objects.requireNonNull(ifNotExists, "ifNotExists");
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRegisterType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RegisterType that = (RegisterType) o;
    return Objects.equals(type, that.type)
        && Objects.equals(name, that.name)
        && ifNotExists == that.ifNotExists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, ifNotExists);
  }

  @Override
  public String toString() {
    return "RegisterType{"
        + "type=" + type
        + ", name='" + name + '\''
        + ", ifNotExists=" + ifNotExists
        + '}';
  }
}
