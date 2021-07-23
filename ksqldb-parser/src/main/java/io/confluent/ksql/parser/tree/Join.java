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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class Join extends Relation {

  private final Relation left;
  private final ImmutableList<JoinedSource> rights;

  public Join(
      final Relation left,
      final List<JoinedSource> rights
  ) {
    this(Optional.empty(), left, rights);
  }

  public Join(
      final Optional<NodeLocation> location,
      final Relation left,
      final List<JoinedSource> rights
  ) {
    super(location);
    this.left = requireNonNull(left, "left");
    this.rights = ImmutableList.copyOf(Objects.requireNonNull(rights, "sources"));
    Preconditions.checkArgument(!rights.isEmpty(), "Cannot join without any right sources!");
  }

  public Relation getLeft() {
    return left;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "rights is ImmutableList")
  public ImmutableList<JoinedSource> getRights() {
    return rights;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public String toString() {
    return "Join{"
        + "left=" + left
        + ", rights=" + rights
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }
    final Join that = (Join) o;
    return Objects.equals(left, that.left)
           && Objects.equals(rights, that.rights);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, rights);
  }
}
