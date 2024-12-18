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
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteQueryResponseHandler extends QueryResponseHandler<BatchedQueryResult> {

  private static final Logger log = LoggerFactory.getLogger(ExecuteQueryResponseHandler.class);

  private final List<Row> rows;
  private final int maxRows;
  private List<String> columnNames;
  private List<ColumnType> columnTypes;
  private Map<String, Integer> columnNameToIndex;
  private AtomicReference<String> serializedConsistencyVector;

  ExecuteQueryResponseHandler(
      final Context context,
      final RecordParser recordParser,
      final BatchedQueryResult cf,
      final int maxRows,
      final AtomicReference<String> serializedCV) {
    super(context, recordParser, cf);
    this.maxRows = maxRows;
    this.rows = new ArrayList<>();
    this.serializedConsistencyVector = Objects.requireNonNull(serializedCV, "serializedCV");
  }

  @Override
  protected void handleMetadata(final QueryResponseMetadata queryResponseMetadata) {
    cf.queryID().complete(queryResponseMetadata.queryId);
    columnNames = queryResponseMetadata.columnNames;
    columnTypes = RowUtil.columnTypesFromStrings(queryResponseMetadata.columnTypes);
    columnNameToIndex = RowUtil.valueToIndexMap(columnNames);
  }

  @Override
  protected void handleRow(final Buffer buff) {
    final JsonObject json = buff.toJsonObject();

    if (!json.containsKey("finalMessage")) {
      if (json.containsKey("row")) {
        if (rows.size() < maxRows) {
          rows.add(new RowImpl(
                  columnNames,
                  columnTypes,
                  new JsonArray((List)((Map<?, ?>) json.getMap().get("row")).get("columns")),
                  columnNameToIndex));
        } else {
          throw new KsqlClientException(
                  "Reached max number of rows that may be returned by executeQuery(). "
                          + "Increase the limit via ClientOptions#setExecuteQueryMaxResultRows(). "
                          + "Current limit: " + maxRows);
        }
      } else if (json.getMap() != null) {
        // This is the serialized consistency vector
        // Don't add it to the result list since the user should not see it
        if (json.getMap().containsKey("consistencyToken")) {
          log.info("Response contains consistency vector " + json);
          serializedConsistencyVector.set((String) json.getMap().get(
              "consistencyToken"));
        } else {
          throw new RuntimeException("Could not decode JSON, expected consistency token: " + json);
        }
      } else {
        throw new RuntimeException("Could not decode JSON: " + json);
      }
    }
  }

  @Override
  protected void doHandleBodyEnd() {
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
