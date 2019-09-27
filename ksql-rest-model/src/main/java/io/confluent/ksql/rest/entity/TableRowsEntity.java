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

package io.confluent.ksql.rest.entity;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableRowsEntity extends KsqlEntity {

  private final LogicalSchema schema;
  private final ImmutableList<List<?>> rows;

  public TableRowsEntity(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("schema") final LogicalSchema schema,
      @JsonProperty("rows") final List<List<?>> rows
  ) {
    super(statementText);
    this.schema = requireNonNull(schema, "schema");
    this.rows = deepCopy(requireNonNull(rows, "rows"));

    rows.forEach(this::validate);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public List<List<?>> getRows() {
    return rows;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableRowsEntity)) {
      return false;
    }
    final TableRowsEntity that = (TableRowsEntity) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(rows, that.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, rows);
  }

  private void validate(final List<?> row) {
    final int expectedSize = schema.key().size() + schema.value().size();
    final int actualSize = row.size();

    if (expectedSize != actualSize) {
      throw new IllegalArgumentException("column count mismatch."
          + " expected: " + expectedSize
          + ", got: " + actualSize
      );
    }
  }

  private static ImmutableList<List<?>> deepCopy(final List<List<?>> rows) {
    final Builder<List<?>> builder = ImmutableList.builder();
    rows.stream()
        .<List<?>>map(ArrayList::new)
        .map(Collections::unmodifiableList)
        .forEach(builder::add);

    return builder.build();
  }
}
