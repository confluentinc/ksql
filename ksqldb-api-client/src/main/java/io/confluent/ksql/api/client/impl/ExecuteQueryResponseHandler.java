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

import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.api.server.protocol.QueryResponseMetadata;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.parsetools.RecordParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteQueryResponseHandler extends QueryResponseHandler<List<Row>> {

  private static final Logger log = LoggerFactory.getLogger(ExecuteQueryResponseHandler.class);

  private final List<Row> rows;
  private final int maxRows;
  private List<String> columnNames;
  private List<String> columnTypes;
  private Map<String, Integer> columnNameToIndex;

  ExecuteQueryResponseHandler(
      final Context context,
      final RecordParser recordParser,
      final CompletableFuture<List<Row>> cf,
      final int maxRows) {
    super(context, recordParser, cf);
    this.maxRows = maxRows;
    this.rows = new ArrayList<>();
  }

  @Override
  protected void handleMetadata(final QueryResponseMetadata queryResponseMetadata) {
    columnNames = queryResponseMetadata.columnNames;
    columnTypes = queryResponseMetadata.columnTypes;
    columnNameToIndex = RowUtil.valueToIndexMap(columnNames);
  }

  @Override
  protected void handleRow(final Buffer buff) {
    final JsonArray values = new JsonArray(buff);
    if (rows.size() < maxRows) {
      rows.add(new RowImpl(columnNames, columnTypes, values, columnNameToIndex));
    } else {
      throw new KsqlRestClientException(
          "Reached max number of rows that may be returned by executeQuery(). "
              + "Increase the limit via ClientOptions#setExecuteQueryMaxResultRows(). "
              + "Current limit: " + maxRows);
    }
  }

  @Override
  protected void handleBodyEnd() {
    if (!hasReadArguments) {
      throw new IllegalStateException("Body ended before metadata received");
    }

    cf.complete(rows);
  }

  @Override
  public void handleExceptionAfterFutureCompleted(final Throwable t) {
    // This should not happen
    log.error("Exceptions should not occur after the future has been completed", t);
  }
}
