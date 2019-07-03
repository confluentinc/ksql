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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.io.IOException;
import java.util.Collection;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WebSocketSubscriber<T> implements Flow.Subscriber<Collection<T>>, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(WebSocketSubscriber.class);
  private final Session session;
  private final ObjectMapper mapper;

  private Flow.Subscription subscription;
  private volatile boolean closed = false;

  WebSocketSubscriber(final Session session, final ObjectMapper mapper) {
    this.session = session;
    this.mapper = mapper;
  }

  public void onSubscribe(final Flow.Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }

  @Override
  public void onNext(final Collection<T> rows) {
    for (final T row : rows) {
      // check if session is closed inside the loop to avoid
      // logging too many async callback errors after close
      if (!closed) {
        try {
          final String buffer = mapper.writeValueAsString(row);
          session.getAsyncRemote().sendText(
              buffer, result -> {
                if (!result.isOK()) {
                  log.warn(
                      "Error sending websocket message for session {}",
                      session.getId(),
                      result.getException()
                  );
                }
              });

        } catch (final JsonProcessingException e) {
          log.warn("Error serializing row in session {}", session.getId(), e);
        }
      }
    }
    if (!closed) {
      subscription.request(1);
    }
  }

  @Override
  public void onError(final Throwable e) {
    log.error("error in session {}", session.getId(), e);
    SessionUtil.closeSilently(
        session,
        CloseCodes.UNEXPECTED_CONDITION,
        "streams exception: " + e.getMessage()
    );
  }

  @Override
  public void onComplete() {
    SessionUtil.closeSilently(session, CloseCodes.NORMAL_CLOSURE, "done");
  }

  @Override
  public void onSchema(final LogicalSchema schema) {
    try {
      session.getBasicRemote().sendText(
          mapper.writeValueAsString(EntityUtil.buildSourceSchemaEntity(schema, true))
      );
    } catch (final IOException e) {
      log.error("Error sending schema", e);
      SessionUtil.closeSilently(session, CloseCodes.PROTOCOL_ERROR, "Unable to send schema");
    }
  }

  @Override
  public void close() {
    closed = true;
    if (subscription != null) {
      subscription.cancel();
    }
  }
}
