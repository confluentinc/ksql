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

import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.confluent.ksql.api.server.protocol.PojoDeserializerErrorHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
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

  public static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg) {
    final ErrorResponse errorResponse = new ErrorResponse(errorCode, errMsg);
    final Buffer buffer = errorResponse.toBuffer();
    response.setStatusCode(statusCode).end(buffer);
  }

  public static void unhandledExceptonHandler(final Throwable t) {
    log.error("Unhandled exception", t);
  }

  public static <T> Optional<T> deserialiseObject(final Buffer buffer,
      final HttpServerResponse response,
      final Class<T> clazz) {
    return PojoCodec.deserialiseObject(buffer, new HttpResponseErrorHandler(response), clazz);
  }

  private static class HttpResponseErrorHandler implements PojoDeserializerErrorHandler {

    private final HttpServerResponse response;

    HttpResponseErrorHandler(final HttpServerResponse response) {
      this.response = response;
    }

    @Override
    public void onMissingParam(final String paramName) {
      handleError(response, 400, ErrorCodes.ERROR_CODE_MISSING_PARAM,
          "No " + paramName + " in arguments");
    }

    @Override
    public void onExtraParam(final String paramName) {
      handleError(response, 400, ErrorCodes.ERROR_CODE_UNKNOWN_PARAM,
          "Unknown arg " + paramName);
    }

    @Override
    public void onInvalidJson() {
      handleError(response, 400, ErrorCodes.ERROR_CODE_MALFORMED_REQUEST,
          "Malformed JSON in request");
    }
  }

}
