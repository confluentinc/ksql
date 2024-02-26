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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_FORBIDDEN;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureHandler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

  @Override
  public void handle(final RoutingContext routingContext) {
    final int statusCode = routingContext.statusCode();
    final Throwable failure = routingContext.failure();
    // Don't log the failure, since it may contain sensitive information meant for the user.
    log.error(
        String.format(
            "Failed to handle request %d %s",
            routingContext.statusCode(),
            routingContext.request().path()
        )
    );


    if (failure != null) {
      if (failure instanceof KsqlApiException) {
        final KsqlApiException ksqlApiException = (KsqlApiException) failure;
        handleError(
            routingContext.response(),
            statusCode,
            ksqlApiException.getErrorCode(),
            ksqlApiException.getMessage(),
            ksqlApiException.getStatement()
        );
      } else {
        final int errorCode;
        if (statusCode == UNAUTHORIZED.code()) {
          errorCode = ERROR_CODE_UNAUTHORIZED;
        } else if (statusCode == FORBIDDEN.code()) {
          errorCode = ERROR_CODE_FORBIDDEN;
        } else {
          errorCode = ERROR_CODE_SERVER_ERROR;
        }
        handleError(
            routingContext.response(),
            statusCode,
            errorCode,
            failure.getMessage(),
            Optional.empty()
        );
      }
    } else {
      routingContext.response().setStatusCode(statusCode).end();
    }
  }

  private static void handleError(final HttpServerResponse response, final int statusCode,
      final int errorCode, final String errMsg, final Optional<String> statement) {
    if (statement.isPresent()) {
      final KsqlStatementErrorMessage errorResponse = new KsqlStatementErrorMessage(
          errorCode, errMsg, statement.get(), new KsqlEntityList());
      final Buffer buffer = ServerUtils.serializeObject(errorResponse);
      response.setStatusCode(statusCode).end(buffer);
    } else {
      final KsqlErrorMessage errorResponse = new KsqlErrorMessage(errorCode, errMsg);
      final Buffer buffer = ServerUtils.serializeObject(errorResponse);
      response.setStatusCode(statusCode).end(buffer);
    }
  }
}
