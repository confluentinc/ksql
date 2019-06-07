/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import javax.ws.rs.core.Response;

public class Errors {
  public static final int HTTP_TO_ERROR_CODE_MULTIPLIER = 100;

  public static final int ERROR_CODE_BAD_REQUEST = toErrorCode(BAD_REQUEST.getStatusCode());
  public static final int ERROR_CODE_BAD_STATEMENT = toErrorCode(BAD_REQUEST.getStatusCode()) + 1;
  public static final int ERROR_CODE_QUERY_ENDPOINT = toErrorCode(BAD_REQUEST.getStatusCode()) + 2;

  public static final int ERROR_CODE_UNAUTHORIZED = toErrorCode(UNAUTHORIZED.getStatusCode());

  public static final int ERROR_CODE_NOT_FOUND = toErrorCode(NOT_FOUND.getStatusCode());

  public static final int ERROR_CODE_SERVER_ERROR =
      toErrorCode(INTERNAL_SERVER_ERROR.getStatusCode());

  public static int toStatusCode(final int errorCode) {
    return errorCode / HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static int toErrorCode(final int statusCode) {
    return statusCode * HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static Response badRequest(final String msg) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, msg))
        .build();
  }

  public static Response badRequest(final Throwable t) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, t))
        .build();
  }

  public static Response badStatement(
      final String msg, final String statementText, final KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(
            new KsqlStatementErrorMessage(ERROR_CODE_BAD_STATEMENT, msg, statementText, entities))
        .build();
  }

  public static Response badStatement(
      final Throwable t, final String statementText, final KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_BAD_STATEMENT, t, statementText, entities))
        .build();
  }

  public static Response queryEndpoint(final String statementText, final KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(
            new KsqlStatementErrorMessage(
                ERROR_CODE_QUERY_ENDPOINT, "SELECT and PRINT queries must use the /query endpoint",
                statementText, entities))
        .build();
  }

  public static Response notFound(final String msg) {
    return Response
        .status(NOT_FOUND)
        .entity(new KsqlErrorMessage(ERROR_CODE_NOT_FOUND, msg))
        .build();
  }

  public static Response serverErrorForStatement(
      final Throwable t, final String statementText, final KsqlEntityList entities) {
    return Response
        .status(INTERNAL_SERVER_ERROR)
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_SERVER_ERROR, t, statementText, entities))
        .build();
  }
}
