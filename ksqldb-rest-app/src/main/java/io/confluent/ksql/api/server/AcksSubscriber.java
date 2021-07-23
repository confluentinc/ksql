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
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.entity.InsertAck;
import io.confluent.ksql.rest.entity.InsertError;
import io.vertx.core.Context;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reactive streams subscriber that subscribes to publishers of acks. As it receive acks it writes
 * them to the HTTP response.
 */
public class AcksSubscriber extends BaseSubscriber<InsertResult> {

  private static final Logger log = LoggerFactory.getLogger(AcksSubscriber.class);
  private static final int REQUEST_BATCH_SIZE = 200;

  private final HttpServerResponse response;
  private final InsertsStreamResponseWriter insertsStreamResponseWriter;
  private Long insertsSent;
  private long acksSent;
  private boolean drainHandlerSet;
  private boolean responseEnded;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public AcksSubscriber(final Context context, final HttpServerResponse response,
      final InsertsStreamResponseWriter insertsStreamResponseWriter) {
    super(context);
    this.response = Objects.requireNonNull(response);
    this.insertsStreamResponseWriter = Objects.requireNonNull(insertsStreamResponseWriter);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(REQUEST_BATCH_SIZE);
  }

  @Override
  public void handleValue(final InsertResult result) {
    checkContext();
    if (responseEnded) {
      return;
    }
    if (result.succeeded()) {
      handleSuccessfulInsert(result);
    } else {
      handleFailedInsert(result);
    }
  }

  @Override
  public void handleComplete() {
    insertsStreamResponseWriter.end();
  }

  @Override
  public void handleError(final Throwable t) {
    log.error("Error in processing inserts", t);
  }

  private void handleSuccessfulInsert(final InsertResult result) {
    insertsStreamResponseWriter.writeInsertResponse(new InsertAck(result.sequenceNumber()));
    acksSent++;
    if (insertsSent != null && insertsSent == acksSent) {
      close();
    } else if (response.writeQueueFull()) {
      if (!drainHandlerSet) {
        response.drainHandler(v -> {
          drainHandlerSet = false;
          checkMakeRequest();
        });
        drainHandlerSet = true;
      }
    } else {
      checkMakeRequest();
    }
  }

  private void handleFailedInsert(final InsertResult result) {
    log.error("Error in processing inserts", result.exception());
    final InsertError insertError;
    final Exception exception = result.exception();
    if (exception instanceof KsqlApiException) {
      insertError = new InsertError(result.sequenceNumber(),
          ((KsqlApiException) exception).getErrorCode(),
          exception.getMessage());
    } else {
      insertError = new InsertError(result.sequenceNumber(), ERROR_CODE_SERVER_ERROR,
          "Error in processing inserts. Check server logs for details.");
    }
    insertsStreamResponseWriter.writeError(insertError).end();
    responseEnded = true;
  }

  private void checkMakeRequest() {
    if (acksSent % REQUEST_BATCH_SIZE == 0) {
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  private void close() {
    insertsStreamResponseWriter.end();
    complete();
  }

  void insertsSent(final long num) {
    this.insertsSent = num;
    if (acksSent == num) {
      close();
    }
  }

}
