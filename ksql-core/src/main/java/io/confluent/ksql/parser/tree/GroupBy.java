/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GroupBy
    extends Node {

  private final boolean isDistinct;
  private final List<GroupingElement> groupingElements;

  public GroupBy(boolean isDistinct, List<GroupingElement> groupingElements) {
    this(Optional.empty(), isDistinct, groupingElements);
  }

  public GroupBy(NodeLocation location, boolean isDistinct,
                 List<GroupingElement> groupingElements) {
    this(Optional.of(location), isDistinct, groupingElements);
  }

  private GroupBy(Optional<NodeLocation> location, boolean isDistinct,
                  List<GroupingElement> groupingElements) {
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
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGroupBy(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupBy groupBy = (GroupBy) o;
    return isDistinct == groupBy.isDistinct &&
           Objects.equals(groupingElements, groupBy.groupingElements);
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
