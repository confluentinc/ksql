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

  public boolean isJoin() {
    return sourceSchemas.size() > 1;
  }
}
