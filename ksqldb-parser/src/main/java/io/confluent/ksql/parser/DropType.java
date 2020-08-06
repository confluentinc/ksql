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

package io.confluent.ksql.parser;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Statement;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class DropType extends Statement implements ExecutableDdlStatement {

  private final String typeName;
  private final boolean ifExists;

  public DropType(
      final Optional<NodeLocation> location,
      final String typeName,
      final boolean ifExists
  ) {
    super(location);
    this.typeName = Objects.requireNonNull(typeName, "typeName");
    this.ifExists = ifExists;
  }

  public String getTypeName() {
    return typeName;
  }

  public boolean getIfExists() {
    return ifExists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DropType that = (DropType) o;
    return Objects.equals(typeName, that.typeName)
        && ifExists == that.ifExists;
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, ifExists);
  }

  @Override
  public String toString() {
    return "DropType{"
        + "typeName='" + typeName + "',"
        + "ifExists=" + ifExists
        + '}';
  }
}
