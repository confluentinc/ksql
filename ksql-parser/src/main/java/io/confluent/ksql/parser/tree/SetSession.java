/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Objects;
import java.util.Optional;

public class SetSession
    extends Statement {

  private final QualifiedName name;
  private final Expression value;

  public SetSession(final QualifiedName name, final Expression value) {
    this(Optional.empty(), name, value);
  }

  public SetSession(final NodeLocation location, final QualifiedName name, final Expression value) {
    this(Optional.of(location), name, value);
  }

  private SetSession(
      final Optional<NodeLocation> location, final QualifiedName name, final Expression value) {
    super(location);
    this.name = name;
    this.value = value;
  }

  public QualifiedName getName() {
    return name;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSetSession(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final SetSession o = (SetSession) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(value, o.value);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("value", value)
        .toString();
  }
}
