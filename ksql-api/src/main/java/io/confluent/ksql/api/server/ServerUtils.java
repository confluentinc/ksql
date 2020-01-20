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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_INVALID_JSON;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static util methods used in the server
 */
public final class ServerUtils {

  private static final Logger log = LoggerFactory.getLogger(ServerUtils.class);

  private ServerUtils() {
  }

  public static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg) {
    final JsonObject errResponse = createErrResponse(errorCode, errMsg);
    response.setStatusCode(statusCode).end(errResponse.toBuffer());
  }

  public static JsonObject createErrResponse(final int errorCode, final String errMsg) {
    return new JsonObject().put("status", "error")
        .put("errorCode", errorCode)
        .put("message", errMsg);
  }

  public static void unhandledExceptonHandler(final Throwable t) {
    log.error("Unhandled exception", t);
  }

  public static JsonObject decodeJsonObject(final Buffer buffer,
      final RoutingContext routingContext) {
    try {
      return new JsonObject(buffer);
    } catch (DecodeException e) {
      handleError(routingContext.response(), 400, ERROR_CODE_INVALID_JSON,
          "Invalid JSON in request args");
      return null;
    }
  }


}
