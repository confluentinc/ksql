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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_HTTP2_ONLY;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.util.KsqlStatementException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static util methods used in the server
 */
public final class ServerUtils {

  private static final Logger log = LoggerFactory.getLogger(ServerUtils.class);

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private ServerUtils() {
  }

  public static <T> Optional<T> deserialiseObject(final Buffer buffer,
      final RoutingContext routingContext,
      final Class<T> clazz) {
    try {
      return Optional.of(OBJECT_MAPPER.readValue(buffer.getBytes(), clazz));
    } catch (JsonParseException | MismatchedInputException e) {
      routingContext
          .fail(BAD_REQUEST.code(),
              new KsqlApiException("Invalid JSON in request: " + e.getMessage(),
                  ERROR_CODE_BAD_REQUEST));
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize buffer", e);
    }
  }

  public static <T> Buffer serializeObject(final T t) {
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(t);
      return Buffer.buffer(bytes);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize buffer", e);
    }
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

  public static boolean checkHttp2(final RoutingContext routingContext) {
    if (routingContext.request().version() != HttpVersion.HTTP_2) {
      routingContext.fail(BAD_REQUEST.code(),
          new KsqlApiException("This endpoint is only available when using HTTP2",
              ERROR_CODE_HTTP2_ONLY));
      return false;
    } else {
      return true;
    }
  }

  static Void handleEndpointException(
      final Throwable t, final RoutingContext routingContext, final String logMsg) {
    if (t instanceof CompletionException) {
      final Throwable actual = t.getCause();
      log.error(logMsg, actual);
      if (actual instanceof KsqlStatementException) {
        routingContext.fail(
            BAD_REQUEST.code(),
            new KsqlApiException(
                ((KsqlStatementException) actual).getUnloggedMessage(),
                ERROR_CODE_BAD_STATEMENT,
                ((KsqlStatementException) actual).getSqlStatement()
            )
        );
        return null;
      } else if (actual instanceof KsqlApiException) {
        routingContext.fail(BAD_REQUEST.code(), actual);
        return null;
      }
    } else {
      log.error(logMsg, t);
    }
    // We don't expose internal error message via public API
    routingContext.fail(INTERNAL_SERVER_ERROR.code(), new KsqlApiException(
        "The server encountered an internal error when processing the query."
            + " Please consult the server logs for more information.",
        ERROR_CODE_SERVER_ERROR));
    return null;
  }
}
