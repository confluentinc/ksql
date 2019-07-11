/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Immutable
public final class TableElements implements Iterable<TableElement> {

  private final ImmutableList<TableElement> elements;

  public static TableElements of(final TableElement... elements) {
    return build(Arrays.stream(elements));
  }

  public static TableElements of(final List<TableElement> elements) {
    return build(elements.stream());
  }

  @Override
  public Iterator<TableElement> iterator() {
    return elements.iterator();
  }

  public Stream<TableElement> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableElements that = (TableElements) o;
    return Objects.equals(elements, that.elements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elements);
  }

  @Override
  public String toString() {
    return elements.toString();
  }

  private TableElements(final ImmutableList<TableElement> elements) {
    this.elements = Objects.requireNonNull(elements, "elements");
  }

  private static TableElements build(final Stream<TableElement> elements) {
    final List<TableElement> valueColumns = elements.collect(Collectors.toList());

    throwOnDuplicateNames(valueColumns, "non-KEY");

    final ImmutableList.Builder<TableElement> builder = ImmutableList.builder();

    builder.addAll(valueColumns);

    return new TableElements(builder.build());
  }

  private static void throwOnDuplicateNames(
      final List<TableElement> columns,
      final String type
  ) {
    final String duplicates = columns.stream()
        .collect(Collectors.groupingBy(TableElement::getName, Collectors.counting()))
        .entrySet()
        .stream()
        .filter(e -> e.getValue() > 1)
        .map(Entry::getKey)
        .collect(Collectors.joining(", "));

    if (!duplicates.isEmpty()) {
      throw new KsqlException("Duplicate " + type + " column names: " + duplicates);
    }
  }
}
