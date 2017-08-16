/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;

public class GroupingSets
    extends GroupingElement {

  private final List<List<QualifiedName>> sets;

  public GroupingSets(List<List<QualifiedName>> groupingSetList) {
    this(Optional.empty(), groupingSetList);
  }

  public GroupingSets(NodeLocation location, List<List<QualifiedName>> sets) {
    this(Optional.of(location), sets);
  }

  private GroupingSets(Optional<NodeLocation> location, List<List<QualifiedName>> sets) {
    super(location);
    requireNonNull(sets);
    checkArgument(!sets.isEmpty(), "grouping sets cannot be empty");
    this.sets = sets;
  }

  @Override
  public List<Set<Expression>> enumerateGroupingSets() {
    return sets.stream()
        .map(groupingSet -> groupingSet.stream()
            .map(QualifiedNameReference::new)
            .collect(Collectors.<Expression>toSet()))
        .collect(collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  @Override
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGroupingSets(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupingSets groupingSets = (GroupingSets) o;
    return Objects.equals(sets, groupingSets.sets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sets);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("sets", sets)
        .toString();
  }
}
