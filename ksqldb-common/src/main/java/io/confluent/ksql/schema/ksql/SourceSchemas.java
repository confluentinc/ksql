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

package io.confluent.ksql.schema.ksql;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper for finding fields in the schemas of one or more aliased sources.
 */
public final class SourceSchemas {

  private final ImmutableMap<SourceName, LogicalSchema> sourceSchemas;

  public SourceSchemas(final Map<SourceName, LogicalSchema> sourceSchemas) {
    this.sourceSchemas = ImmutableMap.copyOf(requireNonNull(sourceSchemas, "sourceSchemas"));

    if (sourceSchemas.isEmpty()) {
      throw new IllegalArgumentException("Must supply at least one schema");
    }
  }

  /**
   * @return {@code true} if there is more than one source schema, i.e. its a join.
   */
  public boolean isJoin() {
    return sourceSchemas.size() > 1;
  }

  /**
   * Find the name of any sources containing the supplied {@code target}.
   *
   * <p>The supplied name can be prefixed with a source name. In which case, only that specific
   * source is checked. If not prefix is present, all sources are checked.
   *
   * @param target the field name to search for. Can be prefixed by source name.
   * @return the set of source names or aliases which contain the supplied {@code target}.
   */
  public Set<SourceName> sourcesWithField(
      final Optional<SourceName> source,
      final ColumnName target
  ) {
    if (!source.isPresent()) {
      return sourceSchemas.entrySet().stream()
          .filter(e -> e.getValue().findColumn(target).isPresent())
          .map(Entry::getKey)
          .collect(Collectors.toSet());
    }

    final SourceName sourceName = source.get();
    final LogicalSchema sourceSchema = sourceSchemas.get(sourceName);
    if (sourceSchema == null) {
      return ImmutableSet.of();
    }

    return sourceSchema.findColumn(target).isPresent()
        ? ImmutableSet.of(sourceName)
        : ImmutableSet.of();
  }

  /**
   * Retrieves the column with the given source name and column name
   * @param sourceName The source name to fetch
   * @param columnName The column name to fetch
   * @return The column
   */
  public Optional<Column> findValueColumn(
      final SourceName sourceName,
      final ColumnName columnName
  ) {
    final LogicalSchema sourceSchema = sourceSchemas.get(sourceName);
    if (sourceSchema == null) {
      return Optional.empty();
    }
    return sourceSchema.findValueColumn(columnName);
  }

  /**
   * Retrieves the column with the given column name.  It searches all sources.  If the column is
   * ambiguous, it throws an error.  Otherwise returns the column, if it's found.
   * @param columnName The column name to fetch
   * @return The column
   */
  public Optional<Column> findValueColumn(
      final ColumnName columnName
  ) {
    final Set<SourceName> sources = sourcesWithField(Optional.empty(), columnName);
    if (sources.size() > 1) {
      throw new KsqlException("Ambiguous field " + columnName.text());
    }
    return sources.stream()
        .findFirst()
        .flatMap(sn -> sourceSchemas.get(sn).findValueColumn(columnName));
  }

  /**
   * Determines if the supplied {@code column} matches a source(s) meta or key fields.
   *
   * <p>The supplied name can be prefixed with a source name. In which case, only that specific
   * source is checked. If no prefix is present, all sources are checked.
   *
   * @param column the field name to search for. Can be prefixed by source name.
   * @return true if this the supplied {@code column} matches a non-value field
   */
  public boolean matchesNonValueField(final Optional<SourceName> source, final ColumnName column) {
    if (!source.isPresent()) {
      return sourceSchemas.values().stream()
          .anyMatch(schema ->
              schema.isMetaColumn(column) || schema.isKeyColumn(column));
    }

    final SourceName sourceName = source.get();
    final LogicalSchema sourceSchema = sourceSchemas.get(sourceName);
    if (sourceSchema == null) {
      throw new IllegalArgumentException("Unknown source: " + sourceName);
    }

    return sourceSchema.isKeyColumn(column) || sourceSchema.isMetaColumn(column);
  }
}
