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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MALFORMED_REQUEST;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MISSING_PARAM;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.confluent.ksql.api.server.protocol.PojoDeserializerErrorHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static util methods used in the server
 */
public final class ServerUtils {

  private static final Logger log = LoggerFactory.getLogger(ServerUtils.class);

  private ServerUtils() {
  }

  public static <T> Optional<T> deserialiseObject(final Buffer buffer,
      final RoutingContext routingContext,
      final Class<T> clazz) {
    return PojoCodec.deserialiseObject(buffer, new HttpResponseErrorHandler(routingContext), clazz);
  }

  /*
  Converts a list of patterns that can include asterisk (E.g. "/ws/*,/foo,/bar/wibble")
  wildcard to a regex pattern
  */
  public static String convertCommaSeparatedWilcardsToRegex(final String csv) {
    final String[] parts = csv.split(",");
    final StringBuilder out = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i].trim();
      part = part.replace(".", "\\.")
          .replace("+", "\\+")
          .replace("?", "\\?")
          .replace("-", "\\-")
          .replace("*", ".*");
      out.append(part);
      if (i != parts.length - 1) {
        out.append('|');
      }
    }
    return out.toString();
  }

  private static class HttpResponseErrorHandler implements PojoDeserializerErrorHandler {

    private final RoutingContext routingContext;

    HttpResponseErrorHandler(final RoutingContext routingContext) {
      this.routingContext = Objects.requireNonNull(routingContext);
    }

    @Override
    public void onMissingParam(final String paramName) {
      routingContext
          .fail(BAD_REQUEST.code(), new KsqlApiException("No " + paramName + " in arguments",
              ERROR_CODE_MISSING_PARAM));
    }

    @Override
    public void onInvalidJson() {
      routingContext
          .fail(BAD_REQUEST.code(), new KsqlApiException("Malformed JSON in request",
              ERROR_CODE_MALFORMED_REQUEST));
    }
  }

}
