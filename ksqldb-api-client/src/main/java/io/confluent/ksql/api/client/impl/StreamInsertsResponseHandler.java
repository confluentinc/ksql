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

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class StreamInsertsResponseHandler
    extends ResponseHandler<CompletableFuture<AcksPublisher>> {

  private static final Logger log = LoggerFactory.getLogger(StreamInsertsResponseHandler.class);

  private final AcksPublisherImpl acksPublisher;
  private boolean paused;

  StreamInsertsResponseHandler(
      final Context context,
      final RecordParser recordParser,
      final CompletableFuture<AcksPublisher> cf,
      final HttpClientRequest request,
      final Publisher<?> insertsPublisher
  ) {
    super(context, recordParser, cf);

    Objects.requireNonNull(request);
    insertsPublisher.subscribe(new StreamInsertsSubscriber(context, request));

    this.acksPublisher = new AcksPublisherImpl(context);
    cf.complete(acksPublisher);
  }

  @Override
  protected void doHandleBodyBuffer(final Buffer buff) {
    final JsonObject jsonObject = new JsonObject(buff);
    final long seqNum = jsonObject.getLong("seq");
    final String status = jsonObject.getString("status");
    if ("ok".equals(status)) {
      final InsertAck ack = new InsertAckImpl(seqNum);
      final boolean full = acksPublisher.accept(ack);
      if (full && !paused) {
        recordParser.pause();
        acksPublisher.drainHandler(this::publisherReceptive);
        paused = true;
      }
    } else if ("error".equals(status)) {
      acksPublisher.handleError(new KsqlClientException(String.format(
          "Received error from /inserts-stream. Inserts sequence number: %d. "
              + "Error code: %d. Message: %s",
          seqNum,
          jsonObject.getInteger("error_code"),
          jsonObject.getString("message")
      )));
    } else {
      throw new IllegalStateException(
          "Unrecognized status response from /inserts-stream: " + status);
    }
  }

  @Override
  protected void doHandleException(final Throwable t) {
    log.error(t);
    acksPublisher.handleError(new Exception(t));
  }

  @Override
  protected void doHandleBodyEnd() {
    acksPublisher.complete();
  }

  private void publisherReceptive() {
    checkContext();

    paused = false;
    recordParser.resume();
  }
}
