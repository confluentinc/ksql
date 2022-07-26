/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class StructAll extends SelectItem {
  private final Expression baseStruct;

  public StructAll(final Expression identifier) {
    this(Optional.empty(), identifier);
  }

  public StructAll(final Optional<NodeLocation> location, final Expression baseStruct) {
    super(location);
    this.baseStruct = requireNonNull(baseStruct, "identifier");
  }

  public Expression getBaseStruct() {
    return baseStruct;
  }

  @Override
  public int hashCode() {
    return baseStruct.hashCode();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StructAll that = (StructAll) o;
    return Objects.equals(baseStruct, that.baseStruct);
  }

  @Override
  public String toString() {
    return "StructAll{"
        + ", baseStruct=" + baseStruct
        + '}';
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitStructAll(this, context);
  }
}
