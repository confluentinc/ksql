/**
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
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GroupBy
    extends Node {

  private final boolean isDistinct;
  private final List<GroupingElement> groupingElements;

  public GroupBy(final boolean isDistinct, final List<GroupingElement> groupingElements) {
    this(Optional.empty(), isDistinct, groupingElements);
  }

  public GroupBy(final NodeLocation location, final boolean isDistinct,
                 final List<GroupingElement> groupingElements) {
    this(Optional.of(location), isDistinct, groupingElements);
  }

  private GroupBy(final Optional<NodeLocation> location, final boolean isDistinct,
                  final List<GroupingElement> groupingElements) {
    super(location);
    this.isDistinct = isDistinct;
    this.groupingElements = ImmutableList.copyOf(requireNonNull(groupingElements));
  }

  public boolean isDistinct() {
    return isDistinct;
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
    return isDistinct == groupBy.isDistinct
           && Objects.equals(groupingElements, groupBy.groupingElements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isDistinct, groupingElements);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("isDistinct", isDistinct)
        .add("groupingElements", groupingElements)
        .toString();
  }
}
