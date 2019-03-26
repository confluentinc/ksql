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
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class DropFunction extends Statement implements ExecutableDdlStatement {

  private final QualifiedName functionName;
  private final boolean exists;

  public DropFunction(
      final QualifiedName functionName,
      final boolean exists
  ) {
    this(Optional.empty(), functionName, exists);
  }

  public DropFunction(
      final Optional<NodeLocation> location,
      final QualifiedName functionName,
      final boolean exists
  ) {
    super(location);
    this.functionName = requireNonNull(functionName, "function name is null");
    this.exists = exists;
  }

  public String getName() {
    return functionName.toString();
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropFunction(this, context);
  }

  @Override
  public int hashCode() {
    return hash(functionName, exists);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final DropFunction o = (DropFunction) obj;
    return Objects.equals(functionName, o.functionName)
           && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("functionName", functionName)
        .add("exists", exists)
        .toString();
  }
}
