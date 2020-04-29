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

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RequestFailureHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RequestFailureHandler.class);

  private RequestFailureHandler() {}

  static void handleFailure(final RoutingContext routingContext) {

    LOG.error(String.format("Failed to handle request %d %s", routingContext.statusCode(),
        routingContext.request().path()),
        routingContext.failure());

    final int statusCode = routingContext.statusCode();
    final Throwable failure = routingContext.failure();

    if (failure != null) {
      if (failure instanceof KsqlApiException) {
        final KsqlApiException ksqlApiException = (KsqlApiException) failure;
        handleError(
            routingContext.response(),
            statusCode,
            ksqlApiException.getErrorCode(),
            ksqlApiException.getMessage()
        );
      } else {
        final int errorCode;
        if (statusCode == UNAUTHORIZED.code()) {
          errorCode = ErrorCodes.ERROR_FAILED_AUTHENTICATION;
        } else if (statusCode == FORBIDDEN.code()) {
          errorCode = ErrorCodes.ERROR_FAILED_AUTHORIZATION;
        } else {
          errorCode = ErrorCodes.ERROR_CODE_INTERNAL_ERROR;
        }
        handleError(
            routingContext.response(),
            statusCode,
            errorCode,
            failure.getMessage()
        );
      }
    } else {
      routingContext.response().setStatusCode(statusCode).end();
    }
  }

  private static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg) {
    final ErrorResponse errorResponse = new ErrorResponse(errorCode, errMsg);
    final Buffer buffer = errorResponse.toBuffer();
    response.setStatusCode(statusCode).end(buffer);
  }
}
