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
  private final String alias;

  public RegisterType(final Optional<NodeLocation> location, final String alias, final Type type) {
    super(location);
    this.alias = Objects.requireNonNull(alias, "alias");
    this.type = Objects.requireNonNull(type, "type");
  }

  public Type getType() {
    return type;
  }

  public String getAlias() {
    return alias;
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
        && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, alias);
  }

  @Override
  public String toString() {
    return "RegisterType{"
        + "type=" + type
        + ", alias='" + alias + '\''
        + '}';
  }
}
