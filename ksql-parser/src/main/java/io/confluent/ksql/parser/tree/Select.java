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

public class Select
    extends Node {

  private final boolean distinct;
  private final List<SelectItem> selectItems;

  public Select(final boolean distinct, final List<SelectItem> selectItems) {
    this(Optional.empty(), distinct, selectItems);
  }

  public Select(
      final NodeLocation location,
      final boolean distinct,
      final List<SelectItem> selectItems) {
    this(Optional.of(location), distinct, selectItems);
  }

  private Select(
      final Optional<NodeLocation> location,
      final boolean distinct,
      final List<SelectItem> selectItems) {
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
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Select select = (Select) o;
    return (distinct == select.distinct)
           && Objects.equals(selectItems, select.selectItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(distinct, selectItems);
  }
}
