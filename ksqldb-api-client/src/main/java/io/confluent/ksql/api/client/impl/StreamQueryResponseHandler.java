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
import io.confluent.ksql.api.client.exception.KsqlException;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamQueryResponseHandler
    extends QueryResponseHandler<CompletableFuture<StreamedQueryResult>> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamQueryResponseHandler.class);

  private StreamedQueryResultImpl queryResult;
  private Map<String, Integer> columnNameToIndex;
  private boolean paused;
  private AtomicReference<String> serializedConsistencyVector;
  private AtomicReference<String> serializedContinuationToken;

  StreamQueryResponseHandler(final Context context, final RecordParser recordParser,
      final CompletableFuture<StreamedQueryResult> cf,
      final AtomicReference<String> serializedCV,
      final AtomicReference<String> serializedCT) {
    super(context, recordParser, cf);
    this.serializedConsistencyVector = Objects.requireNonNull(serializedCV, "serializedCV");
    this.serializedContinuationToken = Objects.requireNonNull(serializedCT, "serializedCT");
  }

  @Override
  protected void handleMetadata(final QueryResponseMetadata queryResponseMetadata) {
    this.queryResult = new StreamedQueryResultImpl(
        context,
        queryResponseMetadata.queryId,
        queryResponseMetadata.columnNames,
        RowUtil.columnTypesFromStrings(queryResponseMetadata.columnTypes)
    );
    this.columnNameToIndex = RowUtil.valueToIndexMap(queryResponseMetadata.columnNames);
    cf.complete(queryResult);
  }

  @Override
  protected void handleRow(final Buffer buff) {
    if (queryResult == null) {
      throw new IllegalStateException("handleRow called before metadata processed");
    }
    final Object json = buff.toJson();
    final Row row;
    if (json instanceof JsonArray) {
      row = new RowImpl(
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
      final JsonObject jsonObject = (JsonObject) json;
      // This is the serialized consistency vector
      // Don't add it to the publisher's buffer since the user should not see it
      if (jsonObject.getMap() != null && jsonObject.getMap().containsKey("consistencyToken")) {
        LOG.info("Response contains consistency vector " + jsonObject);
        serializedConsistencyVector.set((String) ((JsonObject) json).getMap().get(
            "consistencyToken"));
      } else if (jsonObject.getMap() != null && jsonObject.getMap().containsKey("continuationToken")) {
        LOG.info("Response contains continuation token " + jsonObject);
        serializedContinuationToken.set((String) ((JsonObject) json).getMap().get(
                "continuationToken"));
      } else {
        queryResult.handleError(new KsqlException(
            jsonObject.getString("message")
        ));
      }
    } else {
      throw new RuntimeException("Could not decode JSON: " + json);
    }
  }

  @Override
  protected void doHandleBodyEnd() {
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
