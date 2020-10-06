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

package io.confluent.ksql.schema.query;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Pojo for holding data about the physical schemas in use at the different stages within a topology
 * of a query.
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

  private final ImmutableMap<String, SchemaInfo> schemas;

  public static Builder builder() {
    return new Builder();
  }

  private QuerySchemas(final Map<String, SchemaInfo> schemas) {
    this.schemas = ImmutableMap.copyOf(requireNonNull(schemas, "schemas"));
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

  public Map<String, SchemaInfo> getSchemas() {
    return schemas;
  }

  @Override
  public String toString() {
    return schemas.entrySet().stream()
        .map(e -> e.getKey() + " = " + e.getValue())
        .collect(Collectors.joining(System.lineSeparator()));
  }

  @Immutable
  public static final class SchemaInfo {

    private final LogicalSchema schema;
    private final Optional<KeyFormat> keyFormat;
    private final Optional<ValueFormat> valueFormat;

    public SchemaInfo(
        final LogicalSchema schema,
        final Optional<KeyFormat> keyFormat,
        final Optional<ValueFormat> valueFormat
    ) {
      this.schema = requireNonNull(schema, "schema");
      this.keyFormat = requireNonNull(keyFormat, "keyFormat");
      this.valueFormat = requireNonNull(valueFormat, "valueFormat");
    }

    @Override
    public String toString() {
      return "schema=" + schema
          + ", keyFormat=" + keyFormat.map(Object::toString).orElse("?")
          + ", valueFormat=" + valueFormat.map(Object::toString).orElse("?");
    }

    public LogicalSchema schema() {
      return schema;
    }

    public Optional<KeyFormat> keyFormat() {
      return keyFormat;
    }

    public Optional<ValueFormat> valueFormat() {
      return valueFormat;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SchemaInfo that = (SchemaInfo) o;
      return Objects.equals(schema, that.schema)
          && Objects.equals(keyFormat, that.keyFormat)
          && Objects.equals(valueFormat, that.valueFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, keyFormat, valueFormat);
    }

    SchemaInfo merge(
        final Optional<KeyFormat> keyFormat,
        final Optional<ValueFormat> valueFormat
    ) {
      if (this.keyFormat.isPresent() && keyFormat.isPresent()) {
        throw new IllegalStateException("key format already set");
      }

      if (this.valueFormat.isPresent() && valueFormat.isPresent()) {
        throw new IllegalStateException("value format already set");
      }

      return new SchemaInfo(
          schema,
          keyFormat.isPresent() ? keyFormat : this.keyFormat,
          valueFormat.isPresent() ? valueFormat : this.valueFormat
      );
    }
  }

  public static final class Builder {

    private final LinkedHashMap<String, SchemaInfo> schemas = new LinkedHashMap<>();

    public QuerySchemas build() {
      return new QuerySchemas(schemas);
    }

    public void track(
        final String loggerNamePrefix,
        final LogicalSchema schema,
        final KeyFormat keyFormat
    ) {
      track(loggerNamePrefix, schema, Optional.of(keyFormat), Optional.empty());
    }

    public void track(
        final String loggerNamePrefix,
        final LogicalSchema schema,
        final ValueFormat valueFormat
    ) {
      track(loggerNamePrefix, schema, Optional.empty(), Optional.of(valueFormat));
    }

    private void track(
        final String loggerNamePrefix,
        final LogicalSchema schema,
        final Optional<KeyFormat> keyFormat,
        final Optional<ValueFormat> valueFormat
    ) {
      schemas.compute(loggerNamePrefix, (k, existing) -> {
        if (existing == null) {
          return new SchemaInfo(schema, keyFormat, valueFormat);
        }

        if (!existing.schema.equals(schema)) {
          throw new IllegalStateException("Inconsistent schema: "
              + "existing: " + existing.schema + ", new: " + schema);
        }

        return existing.merge(keyFormat, valueFormat);
      });
    }
  }
}
