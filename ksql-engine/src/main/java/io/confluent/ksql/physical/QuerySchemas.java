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

package io.confluent.ksql.physical;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.DefaultSchemaFormatter;
import io.confluent.ksql.schema.connect.SchemaFormatter;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

/**
 * Pojo for holding data about the schemas in use at the different states within a topology of a
 * query.
 *
 * <p>Contains an ordered mapping of 'logger name prefix' to the schema used,
 * where the logger name prefix can be used to map the schema to a stage in the topology.
 */
@Immutable
public final class QuerySchemas {

  private final LinkedHashMap<String, Schema> schemas;
  private final SchemaFormatter schemaFormatter;

  public static QuerySchemas of(final LinkedHashMap<String, Schema> schemas) {
    return new QuerySchemas(schemas, new DefaultSchemaFormatter());
  }

  @VisibleForTesting
  QuerySchemas(
      final LinkedHashMap<String, Schema> schemas,
      final SchemaFormatter schemaFormatter
  ) {
    this.schemas = new LinkedHashMap<>(Objects.requireNonNull(schemas, "schemas"));
    this.schemaFormatter = Objects.requireNonNull(schemaFormatter, "schemaFormatter");
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

  @Override
  public String toString() {
    return schemas.entrySet().stream()
        .map(e -> e.getKey() + " = " + schemaFormatter.format(e.getValue()))
        .collect(Collectors.joining(System.lineSeparator()));
  }
}
