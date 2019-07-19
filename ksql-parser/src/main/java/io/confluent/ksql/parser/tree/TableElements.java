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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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

  public LogicalSchema toLogicalSchema() {
    if (Iterables.isEmpty(this)) {
      throw new KsqlException("No columns supplied.");
    }

    final SchemaBuilder keySchema = SchemaBuilder.struct();
    final SchemaBuilder valueSchema = SchemaBuilder.struct();
    for (final TableElement tableElement : this) {
      final String fieldName = tableElement.getName();
      final Schema fieldSchema = SchemaConverters.sqlToLogicalConverter()
          .fromSqlType(tableElement.getType().getSqlType());

      if (tableElement.getNamespace() == Namespace.KEY) {
        keySchema.field(fieldName, fieldSchema);
      } else {
        valueSchema.field(fieldName, fieldSchema);
      }
    }

    if (keySchema.fields().isEmpty()) {
      keySchema.field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA);
    }

    return LogicalSchema.of(keySchema.build(), valueSchema.build());
  }

  private TableElements(final ImmutableList<TableElement> elements) {
    this.elements = Objects.requireNonNull(elements, "elements");
  }

  private static TableElements build(final Stream<TableElement> elements) {
    final Map<Boolean, List<TableElement>> split = splitByElementType(elements);

    final List<TableElement> keyColumns = split.getOrDefault(Boolean.TRUE, ImmutableList.of());
    final List<TableElement> valueColumns = split.getOrDefault(Boolean.FALSE, ImmutableList.of());

    throwOnDuplicateNames(keyColumns, "KEY");
    throwOnDuplicateNames(valueColumns, "non-KEY");

    final long numKeyColumns = keyColumns.size();

    if (numKeyColumns > 1) {
      throw new KsqlException("KSQL does not yet support multiple KEY columns");
    }

    if (numKeyColumns == 1
        && keyColumns.get(0).getType().getSqlType().baseType() != SqlBaseType.STRING) {
      throw new KsqlException("KEY columns must be of type STRING: " + keyColumns.get(0).getName());
    }

    final ImmutableList.Builder<TableElement> builder = ImmutableList.builder();

    builder.addAll(keyColumns);
    builder.addAll(valueColumns);

    return new TableElements(builder.build());
  }

  private static Map<Boolean, List<TableElement>> splitByElementType(
      final Stream<TableElement> elements
  ) {
    final List<TableElement> keyFields = new ArrayList<>();
    final List<TableElement> valueFields = new ArrayList<>();

    elements.forEach(element -> {
      if (element.getNamespace() == Namespace.VALUE) {
        valueFields.add(element);
        return;
      }

      if (!valueFields.isEmpty()) {
        throw new KsqlException("KEY column declared after VALUE column: " + element.getName()
            + System.lineSeparator()
            + "All KEY columns must be declared before any VALUE column(s).");
      }

      keyFields.add(element);
    });

    return ImmutableMap.of(
        Boolean.TRUE, keyFields,
        Boolean.FALSE, valueFields
    );
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
