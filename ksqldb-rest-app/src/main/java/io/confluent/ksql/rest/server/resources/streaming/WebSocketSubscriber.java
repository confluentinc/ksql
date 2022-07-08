/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.NORMAL_CLOSURE;
import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.PROTOCOL_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.vertx.core.http.ServerWebSocket;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WebSocketSubscriber<T> implements Flow.Subscriber<Collection<T>>, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(WebSocketSubscriber.class);
  private final ServerWebSocket websocket;

  private Flow.Subscription subscription;
  private volatile boolean closed;
  private volatile boolean drainHandlerSet;
  private Optional<MetricsCallbackHolder> metricsCallbackHolderOptional = Optional.empty();
  private long startTimeNanos;

  WebSocketSubscriber(final ServerWebSocket websocket) {
    this.websocket = websocket;
  }

  public void onSubscribe(final Flow.Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  public void onSubscribe(final Flow.Subscription subscription,
                          final MetricsCallbackHolder metricsCallbackHolder,
                          final long startTimeNanos) {
    this.subscription = subscription;
    subscription.request(1);
    metricsCallbackHolderOptional = Optional.of(metricsCallbackHolder);
    this.startTimeNanos = startTimeNanos;
  }


  @Override
  public void onNext(final Collection<T> rows) {
    for (final T row : rows) {
      // check if session is closed inside the loop to avoid
      // logging too many async callback errors after close
      if (!closed) {
        try {
          final String buffer = ApiJsonMapper.INSTANCE.get().writeValueAsString(row);
          websocket.writeTextMessage(buffer);
          if (websocket.writeQueueFull()) {
            drainHandlerSet = true;
            websocket.drainHandler(v -> websocketDrained());
          }
        } catch (final JsonProcessingException e) {
          log.warn("Error serializing row to websocket", e);
        }
      }
    }
    checkRequestTokens();
  }

  @Override
  public void onError(final Throwable e) {
    log.error("error in websocket", e);

    final String msg = e.getMessage() == null || e.getMessage().trim().isEmpty()
        ? "KSQL exception: " + e.getClass().getSimpleName()
        : e.getMessage();

    metricsCallbackHolderOptional.ifPresent(mc -> mc.reportMetrics(0, 0, 0, startTimeNanos));
    SessionUtil.closeSilently(websocket, INTERNAL_SERVER_ERROR.code(), msg);
  }

  @Override
  public void onComplete() {
    // We don't have the status code , request and response size in bytes
    metricsCallbackHolderOptional.ifPresent(mc -> mc.reportMetrics(0, 0, 0, startTimeNanos));
    SessionUtil.closeSilently(websocket, NORMAL_CLOSURE.code(), "done");
  }

  @Override
  public void onSchema(final LogicalSchema schema) {
    try {
      websocket
          .writeTextMessage(ApiJsonMapper.INSTANCE.get()
              .writeValueAsString(EntityUtil.buildSourceSchemaEntity(schema)));
    } catch (final IOException e) {
      log.error("Error sending schema", e);
      SessionUtil.closeSilently(websocket, PROTOCOL_ERROR.code(), "Unable to send schema");
    }
  }

  @Override
  public void close() {
    closed = true;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void checkRequestTokens() {
    if (!closed && !drainHandlerSet) {
      subscription.request(1);
    }
  }

  private void websocketDrained() {
    drainHandlerSet = false;
    checkRequestTokens();
  }

}
