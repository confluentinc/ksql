/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.Row;
import java.util.List;
import java.util.Objects;

public class BatchedQueryResultImpl implements BatchedQueryResult {

  private final String queryId;
  private final List<String> columnNames;
  private final List<ColumnType> columnTypes;
  private final List<Row> rows;

  BatchedQueryResultImpl(
      final String queryId,
      final List<String> columnNames,
      final List<ColumnType> columnTypes,
      final List<Row> rows
  ) {
    this.queryId = queryId;
    this.columnNames = Objects.requireNonNull(columnNames);
    this.columnTypes = Objects.requireNonNull(columnTypes);
    this.rows = Objects.requireNonNull(rows);
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public List<ColumnType> columnTypes() {
    return columnTypes;
  }

  @Override
  public String queryID() {
    return queryId;
  }

  @Override
  public List<Row> rows() {
    return rows;
  }

}
