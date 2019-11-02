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
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Immutable
public final class TableElements implements Iterable<TableElement> {

  private final ImmutableList<TableElement> elements;

  public static TableElements of(final TableElement... elements) {
    return new TableElements(ImmutableList.copyOf(elements));
  }

  public static TableElements of(final List<TableElement> elements) {
    return new TableElements(ImmutableList.copyOf(elements));
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

  /**
   * @param withImplicitColumns controls if schema has implicit columns such as ROWTIME or ROWKEY.
   * @return the logical schema.
   */
  public LogicalSchema toLogicalSchema(final boolean withImplicitColumns) {
    if (Iterables.isEmpty(this)) {
      throw new KsqlException("No columns supplied.");
    }

    final Builder builder = withImplicitColumns
        ? LogicalSchema.builder()
        : LogicalSchema.builder().noImplicitColumns();

    for (final TableElement tableElement : this) {
      addField(tableElement, builder);
    }

    return builder.build();
  }

  private void addField(final TableElement tableElement, final Builder builder) {
    final Optional<SourceName> source = tableElement.getSource();
    final ColumnName fieldName = tableElement.getName();
    final SqlType fieldType = tableElement.getType().getSqlType();

    if (tableElement.getNamespace() == Namespace.KEY) {
      if (source.isPresent()) {
        builder.keyColumn(source.get(), fieldName, fieldType);
      } else {
        builder.keyColumn(fieldName, fieldType);
      }
    } else {
      if (source.isPresent()) {
        builder.valueColumn(source.get(), fieldName, fieldType);
      } else {
        builder.valueColumn(fieldName, fieldType);
      }
    }
  }

  private TableElements(final ImmutableList<TableElement> elements) {
    this.elements = Objects.requireNonNull(elements, "elements");

    throwOnDuplicateNames();
  }

  private void throwOnDuplicateNames() {
    final String duplicates = elements.stream()
        .collect(Collectors.groupingBy(
            e -> ImmutableList.of(e.getSource(), e.getName(), e.getNamespace()),
            Collectors.counting()))
        .entrySet()
        .stream()
        .filter(e -> e.getValue() > 1)
        .map(Entry::getKey)
        .map(l -> String.format("%s.%s %s", l.get(0), l.get(1), l.get(2)))
        .collect(Collectors.joining(", "));

    if (!duplicates.isEmpty()) {
      throw new KsqlException("Duplicate column names: " + duplicates);
    }
  }
}
