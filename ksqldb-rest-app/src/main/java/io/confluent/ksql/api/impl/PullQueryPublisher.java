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

package io.confluent.ksql.api.impl;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.entity.TableRows;
import io.vertx.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PullQueryPublisher extends BufferedPublisher<GenericRow> implements QueryPublisher {

  private final List<String> columnNames;
  private final List<String> columnTypes;

  public PullQueryPublisher(final Context ctx, final TableRows tableRows,
      final List<String> columnNames, final List<String> columnTypes) {
    super(ctx, toGenericRows(tableRows));
    this.columnNames = Objects.requireNonNull(columnNames);
    this.columnTypes = Objects.requireNonNull(columnTypes);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<GenericRow> toGenericRows(final TableRows tableRows) {
    final List<GenericRow> genericRows = new ArrayList<>(tableRows.getRows().size());
    for (List row : tableRows.getRows()) {
      final GenericRow genericRow = GenericRow.fromList(row);
      genericRows.add(genericRow);
    }
    return genericRows;
  }

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public List<String> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public boolean isPullQuery() {
    return true;
  }
}
