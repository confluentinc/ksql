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

public class Select
    extends Node {

  private final boolean distinct;
  private final List<SelectItem> selectItems;

  public Select(boolean distinct, List<SelectItem> selectItems) {
    this(Optional.empty(), distinct, selectItems);
  }

  public Select(NodeLocation location, boolean distinct, List<SelectItem> selectItems) {
    this(Optional.of(location), distinct, selectItems);
  }

  private Select(Optional<NodeLocation> location, boolean distinct, List<SelectItem> selectItems) {
    super(location);
    this.distinct = distinct;
    this.selectItems = ImmutableList.copyOf(requireNonNull(selectItems, "selectItems"));
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSelect(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("distinct", distinct)
        .add("selectItems", selectItems)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Select select = (Select) o;
    return (distinct == select.distinct) &&
           Objects.equals(selectItems, select.selectItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(distinct, selectItems);
  }
}
