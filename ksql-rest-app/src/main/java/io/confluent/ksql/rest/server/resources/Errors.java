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

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class Errors {
  public static final int HTTP_TO_ERROR_CODE_MULTIPLIER = 100;

  public static final int ERROR_CODE_BAD_REQUEST = toErrorCode(BAD_REQUEST.getStatusCode());
  public static final int ERROR_CODE_BAD_STATEMENT = toErrorCode(BAD_REQUEST.getStatusCode()) + 1;
  public static final int ERROR_CODE_QUERY_ENDPOINT = toErrorCode(BAD_REQUEST.getStatusCode()) + 2;

  public static final int ERROR_CODE_UNAUTHORIZED = toErrorCode(UNAUTHORIZED.getStatusCode());

  public static final int ERROR_CODE_NOT_FOUND = toErrorCode(NOT_FOUND.getStatusCode());

  public static final int ERROR_CODE_SERVER_ERROR =
      toErrorCode(INTERNAL_SERVER_ERROR.getStatusCode());

  public static int toStatusCode(int errorCode) {
    return errorCode / HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static int toErrorCode(int statusCode) {
    return statusCode * HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static Response badRequest(String msg) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, msg))
        .build();
  }

  public static Response badRequest(Throwable t) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, t))
        .build();
  }

  public static Response badStatement(String msg, String statementText, KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(
            new KsqlStatementErrorMessage(ERROR_CODE_BAD_STATEMENT, msg, statementText, entities))
        .build();
  }

  public static Response badStatement(Throwable t, String statementText, KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_BAD_STATEMENT, t, statementText, entities))
        .build();
  }

  public static Response queryEndpoint(String statementText, KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(
            new KsqlStatementErrorMessage(
                ERROR_CODE_QUERY_ENDPOINT, "SELECT and PRINT queries must use the /query endpoint",
                statementText, entities))
        .build();
  }

  public static Response notFound(String msg) {
    return Response
        .status(NOT_FOUND)
        .entity(new KsqlErrorMessage(ERROR_CODE_NOT_FOUND, msg))
        .build();
  }

  public static Response serverErrorForStatement(
      Throwable t, String statementText, KsqlEntityList entities) {
    return Response
        .status(INTERNAL_SERVER_ERROR)
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_SERVER_ERROR, t, statementText, entities))
        .build();
  }
}
