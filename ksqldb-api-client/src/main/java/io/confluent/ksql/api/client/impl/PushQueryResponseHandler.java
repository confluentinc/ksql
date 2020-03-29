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

import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.server.protocol.QueryResponseMetadata;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.parsetools.RecordParser;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class PushQueryResponseHandler extends QueryResponseHandler<QueryResult> {

  private QueryResultImpl queryResult;
  private boolean paused;

  PushQueryResponseHandler(final Context context, final RecordParser recordParser,
      final CompletableFuture<QueryResult> cf) {
    super(context, recordParser, cf);
  }

  @Override
  protected void handleMetadata(final QueryResponseMetadata queryResponseMetadata) {
    if (queryResponseMetadata.queryId == null || queryResponseMetadata.queryId.isEmpty()) {
      cf.completeExceptionally(
          new KsqlRestClientException("Expecting push query but was pull query"));
      return;
    }

    this.queryResult = new QueryResultImpl(context, queryResponseMetadata.queryId,
        Collections.unmodifiableList(queryResponseMetadata.columnNames),
        Collections.unmodifiableList(queryResponseMetadata.columnTypes));
    cf.complete(queryResult);
  }

  @Override
  protected void handleRow(final Buffer buff) {
    if (queryResult == null) {
      throw new IllegalStateException("handleRow called before metadata processed");
    }

    final JsonArray values = new JsonArray(buff);
    final Row row = new RowImpl(queryResult.columnNames(), queryResult.columnTypes(), values);
    final boolean full = queryResult.accept(row);
    if (full && !paused) {
      recordParser.pause();
      queryResult.drainHandler(this::publisherReceptive);
      paused = true;
    }
  }

  @Override
  protected void handleBodyEnd() {
  }

  private void publisherReceptive() {
    paused = false;
    recordParser.resume();
  }
}
