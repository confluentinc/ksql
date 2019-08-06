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
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class GroupBy extends AstNode {

  private final ImmutableList<GroupingElement> groupingElements;

  public GroupBy(final List<GroupingElement> groupingElements) {
    this(Optional.empty(), groupingElements);
  }

  public GroupBy(
      final Optional<NodeLocation> location,
      final List<GroupingElement> groupingElements
  ) {
    super(location);
    this.groupingElements = ImmutableList
        .copyOf(requireNonNull(groupingElements, "groupingElements"));
  }

  public List<GroupingElement> getGroupingElements() {
    return groupingElements;
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitGroupBy(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GroupBy groupBy = (GroupBy) o;
    return Objects.equals(groupingElements, groupBy.groupingElements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupingElements);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("groupingElements", groupingElements)
        .toString();
  }
}
