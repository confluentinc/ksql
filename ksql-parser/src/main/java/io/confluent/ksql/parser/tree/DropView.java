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

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Objects;
import java.util.Optional;

public class DropView
    extends Statement {

  private final QualifiedName name;
  private final boolean exists;

  public DropView(final QualifiedName name, final boolean exists) {
    this(Optional.empty(), name, exists);
  }

  public DropView(final NodeLocation location, final QualifiedName name, final boolean exists) {
    this(Optional.of(location), name, exists);
  }

  private DropView(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final boolean exists) {
    super(location);
    this.name = name;
    this.exists = exists;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropView(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, exists);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final DropView o = (DropView) obj;
    return Objects.equals(name, o.name)
           && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("exists", exists)
        .toString();
  }
}
