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

package io.confluent.ksql.analyzer;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;

/**
 * Helper for finding fields in the schemas of one or more aliased sources.
 */
final class SourceSchemas {

  private final Map<String, LogicalSchema> sourceSchemas;

  SourceSchemas(final Map<String, LogicalSchema> sourceSchemas) {
    this.sourceSchemas = ImmutableMap.copyOf(requireNonNull(sourceSchemas, "sourceSchemas"));

    if (sourceSchemas.isEmpty()) {
      throw new IllegalArgumentException("Must supply at least one schema");
    }
  }

  /**
   * @return {@code true} if there is more than one source schema, i.e. its a join.
   */
  boolean isJoin() {
    return sourceSchemas.size() > 1;
  }

  /**
   * Find the name of any sources containing the supplied {@code fieldName}.
   *
   * <p>The supplied name can be prefixed with a source name. In which case, only that specific
   * source is checked. If not prefix is present, all sources are checked.
   *
   * @param fieldName the field name to search for. Can be prefixed by source name.
   * @return the set of source names or aliases which contain the supplied {@code fieldName}.
   */
  Set<String> sourcesWithField(final String fieldName) {

    final Optional<String> maybeSourceName = SchemaUtil.getFieldNameAlias(fieldName);
    if (!maybeSourceName.isPresent()) {
      return sourceSchemas.entrySet().stream()
          .filter(e -> e.getValue().findField(fieldName).isPresent())
          .map(Entry::getKey)
          .collect(Collectors.toSet());
    }

    final String sourceName = maybeSourceName.get();
    final String baseFieldName = SchemaUtil.getFieldNameWithNoAlias(fieldName);

    final LogicalSchema sourceSchema = sourceSchemas.get(sourceName);
    if (sourceSchema == null) {
      return ImmutableSet.of();
    }

    return sourceSchema.findField(baseFieldName).isPresent()
        ? ImmutableSet.of(sourceName)
        : ImmutableSet.of();
  }

  /**
   * Determines if the supplied {@code fieldName} matches a source(s) meta or key fields.
   *
   * <p>The supplied name can be prefixed with a source name. In which case, only that specific
   * source is checked. If no prefix is present, all sources are checked.
   *
   * @param fieldName the field name to search for. Can be prefixed by source name.
   * @return true if this the supplied {@code fieldName} matches a non-value field
   */
  boolean matchesNonValueField(final String fieldName) {
    final Optional<String> maybeSourceName = SchemaUtil.getFieldNameAlias(fieldName);
    if (!maybeSourceName.isPresent()) {
      return sourceSchemas.values().stream()
          .anyMatch(schema -> nonValueFieldNames(schema).contains(fieldName));
    }

    final String sourceName = maybeSourceName.get();
    final String baseFieldName = SchemaUtil.getFieldNameWithNoAlias(fieldName);

    final LogicalSchema sourceSchema = sourceSchemas.get(sourceName);
    if (sourceSchema == null) {
      throw new IllegalArgumentException("Unknown source: " + sourceName);
    }

    return nonValueFieldNames(sourceSchema).contains(baseFieldName);
  }

  private static Set<String> nonValueFieldNames(final LogicalSchema schema) {
    final Set<String> fieldNames = schema.metaFields().stream()
        .map(Field::name)
        .collect(Collectors.toSet());

    schema.keyFields().stream()
        .map(Field::name)
        .forEach(fieldNames::add);

    return fieldNames;
  }
}
