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

package io.confluent.ksql.util;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Pojo for holding data about the persistence schemas in use at the different stages within a
 * topology of a query.
 *
 * <p>Contains an ordered mapping of 'logger name prefix' to the schema used,
 * where the logger name prefix can be used to map the schema to a stage in the topology.
 *
 * <p>This class is predominately used in the {@code QueryTranslationTest} in the
 * ksql-functional-tests module to ensure the schemas of data persisted to topics doesn't change
 * between releases.
 */
@Immutable
public final class QuerySchemas {

  @EffectivelyImmutable
  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(word -> false, Option.APPEND_NOT_NULL);

  @EffectivelyImmutable
  private final SchemaFormatter schemaFormatter;
  private final ImmutableList<Entry> schemas;

  public static QuerySchemas of(final LinkedHashMap<String, PersistenceSchema> schemas) {
    return new QuerySchemas(schemas, FORMATTER);
  }

  @VisibleForTesting
  QuerySchemas(
      final LinkedHashMap<String, PersistenceSchema> schemas,
      final SchemaFormatter schemaFormatter
  ) {
    this(
        schemas.entrySet().stream()
            .map(e -> new Entry(e.getKey(), e.getValue()))
            .collect(Collectors.toList()),
        schemaFormatter
    );
  }

  private QuerySchemas(
      final List<Entry> schemas,
      final SchemaFormatter schemaFormatter
  ) {
    this.schemas = ImmutableList.copyOf(requireNonNull(schemas, "schemas"));
    this.schemaFormatter = requireNonNull(schemaFormatter, "schemaFormatter");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QuerySchemas that = (QuerySchemas) o;
    return Objects.equals(schemas, that.schemas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemas);
  }

  public Map<String, String> getSchemasDescription() {
    final ImmutableMap.Builder<String, String> builder = new Builder<>();
    for (final Entry e : schemas) {
      builder.put(e.loggerNamePrefix, schemaFormatter.format(e.schema.serializedSchema()));
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return schemas.stream()
        .map(e -> e.loggerNamePrefix + " = " + schemaFormatter.format(e.schema.serializedSchema()))
        .collect(Collectors.joining(System.lineSeparator()));
  }

  @Immutable
  private static final class Entry {

    private final String loggerNamePrefix;
    private final PersistenceSchema schema;

    private Entry(
        final String loggerNamePrefix,
        final PersistenceSchema schema
    ) {
      this.loggerNamePrefix = requireNonNull(loggerNamePrefix, "loggerNamePrefix");
      this.schema = requireNonNull(schema, "schema");
    }
  }
}
