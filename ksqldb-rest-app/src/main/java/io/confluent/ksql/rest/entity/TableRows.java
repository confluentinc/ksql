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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TableRows {

  private final LogicalSchema schema;
  private final QueryId queryId;
  private final ImmutableList<List<?>> rows;
  private final String statementText;

  public TableRows(
      final String statementText,
      final QueryId queryId,
      final LogicalSchema schema,
      final List<List<?>> rows
  ) {
    this.statementText = requireNonNull(statementText, "statementText");
    this.schema = requireNonNull(schema, "schema");
    this.queryId = requireNonNull(queryId, "queryId");
    this.rows = deepCopy(requireNonNull(rows, "rows"));

    rows.forEach(this::validate);
  }

  public String getStatementText() {
    return statementText;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<List<?>> getRows() {
    return rows;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableRows that = (TableRows) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(rows, that.rows)
        && Objects.equals(statementText, that.statementText);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, queryId, rows, statementText);
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
