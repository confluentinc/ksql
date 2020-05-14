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
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.api.server.protocol.QueryResponseMetadata;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamQueryResponseHandler extends QueryResponseHandler<StreamedQueryResult> {

  private static final Logger log = LoggerFactory.getLogger(StreamQueryResponseHandler.class);

  private StreamedQueryResultImpl queryResult;
  private Map<String, Integer> columnNameToIndex;
  private boolean paused;

  StreamQueryResponseHandler(final Context context, final RecordParser recordParser,
      final CompletableFuture<StreamedQueryResult> cf) {
    super(context, recordParser, cf);
  }

  @Override
  protected void handleMetadata(final QueryResponseMetadata queryResponseMetadata) {
    this.queryResult = new StreamedQueryResultImpl(context, queryResponseMetadata.queryId,
        Collections.unmodifiableList(queryResponseMetadata.columnNames),
        RowUtil.columnTypesFromStrings(queryResponseMetadata.columnTypes));
    this.columnNameToIndex = RowUtil.valueToIndexMap(queryResponseMetadata.columnNames);
    cf.complete(queryResult);
  }

  @Override
  protected void handleRow(final Buffer buff) {
    if (queryResult == null) {
      throw new IllegalStateException("handleRow called before metadata processed");
    }

    final Object json = buff.toJson();
    if (json instanceof JsonArray) {
      final Row row = new RowImpl(
          queryResult.columnNames(),
          queryResult.columnTypes(),
          (JsonArray) json,
          columnNameToIndex
      );
      final boolean full = queryResult.accept(row);
      if (full && !paused) {
        recordParser.pause();
        queryResult.drainHandler(this::publisherReceptive);
        paused = true;
      }
    } else if (json instanceof JsonObject) {
      final JsonObject error = (JsonObject) json;
      if ("error".equals(error.getString("status"))) {
        queryResult.handleError(new KsqlApiException(
            error.getString("message"),
            error.getInteger("errorCode"))
        );
      } else {
        throw new RuntimeException("Unexpected response from server: " + error);
      }
    } else {
      throw new RuntimeException("Could not decode JSON: " + json);
    }
  }

  @Override
  protected void handleBodyEnd() {
    queryResult.complete();
  }

  @Override
  public void handleExceptionAfterFutureCompleted(final Throwable t) {
    queryResult.handleError(new Exception(t));
  }

  private void publisherReceptive() {
    checkContext();

    paused = false;
    recordParser.resume();
  }
}
