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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.ks.NotUpToBoundException;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a reactive streams subscriber which receives a stream of results from a publisher which
 * is implemented by the back-end. The results are then written to the HTTP2 response.
 */
public class QuerySubscriber extends BaseSubscriber<KeyValueMetadata<List<?>, GenericRow>> {

  private static final Logger log = LoggerFactory.getLogger(QuerySubscriber.class);
  private static final int REQUEST_BATCH_SIZE = 200;

  private final HttpServerResponse response;
  private final QueryStreamResponseWriter queryStreamResponseWriter;
  private final Supplier<Boolean> hitLimit;
  private int tokens;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public QuerySubscriber(final Context context, final HttpServerResponse response,
      final QueryStreamResponseWriter queryStreamResponseWriter,
      final Supplier<Boolean> hitLimit) {
    super(context);
    this.response = Objects.requireNonNull(response);
    this.queryStreamResponseWriter = Objects.requireNonNull(queryStreamResponseWriter);
    this.hitLimit = hitLimit;
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkMakeRequest();
  }

  @Override
  public void handleValue(final KeyValueMetadata<List<?>, GenericRow> row) {
    if (row.getRowMetadata().isPresent() && row.getRowMetadata().get().isStandaloneRow()) {
      // Only one of the metadata are present at a time
      if (row.getRowMetadata().get().getPushOffsetsRange().isPresent()) {
        queryStreamResponseWriter.writeContinuationToken(new PushContinuationToken(
            row.getRowMetadata().get().getPushOffsetsRange().get().serialize()));
      } else if (row.getRowMetadata().get().getConsistencyOffsetVector().isPresent()) {
        queryStreamResponseWriter.writeConsistencyToken(new ConsistencyToken(
            row.getRowMetadata().get().getConsistencyOffsetVector().get().serialize()));
      }
    } else {
      queryStreamResponseWriter.writeRow(row);
    }
    tokens--;
    if (response.writeQueueFull()) {
      response.drainHandler(v -> checkMakeRequest());
    } else {
      checkMakeRequest();
    }
  }

  private void checkMakeRequest() {
    if (tokens == 0) {
      tokens = REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  @Override
  public void handleError(final Throwable t) {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(t);
    for (Throwable s: t.getSuppressed()) {
      if (s instanceof NotUpToBoundException) {
        stringBuilder.append(" Failed to get value from materialized table, reason: "
                                 + "NOT_UP_TO_BOUND");
      } else {
        stringBuilder.append(s.getMessage());
      }
    }
    final KsqlErrorMessage errorResponse = new KsqlErrorMessage(
        ERROR_CODE_SERVER_ERROR, stringBuilder.toString());
    log.error("Error in processing query {}", stringBuilder, t);
    queryStreamResponseWriter.writeError(errorResponse).end();
  }

  @Override
  public void handleComplete() {
    if (hitLimit.get()) {
      queryStreamResponseWriter.writeLimitMessage();
    } else {
      queryStreamResponseWriter.writeCompletionMessage();
    }
    queryStreamResponseWriter.end();
  }

}