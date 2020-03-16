/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_REQUIRED;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import java.util.Objects;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.TopicAuthorizationException;


public final class Errors {
  private static final int HTTP_TO_ERROR_CODE_MULTIPLIER = 100;

  public static final int ERROR_CODE_BAD_REQUEST = toErrorCode(BAD_REQUEST.getStatusCode());
  public static final int ERROR_CODE_BAD_STATEMENT = toErrorCode(BAD_REQUEST.getStatusCode()) + 1;
  private static final int ERROR_CODE_QUERY_ENDPOINT = toErrorCode(BAD_REQUEST.getStatusCode()) + 2;

  public static final int ERROR_CODE_UNAUTHORIZED = toErrorCode(UNAUTHORIZED.getStatusCode());

  public static final int ERROR_CODE_FORBIDDEN = toErrorCode(FORBIDDEN.getStatusCode());
  public static final int ERROR_CODE_FORBIDDEN_KAFKA_ACCESS =
      toErrorCode(FORBIDDEN.getStatusCode()) + 1;

  public static final int ERROR_CODE_SCHEMA_REGISTRY_UNCOFIGURED =
      toErrorCode(PRECONDITION_REQUIRED.getStatusCode()) + 1;

  public static final int ERROR_CODE_NOT_FOUND = toErrorCode(NOT_FOUND.getStatusCode());

  public static final int ERROR_CODE_SERVER_SHUTTING_DOWN =
      toErrorCode(SERVICE_UNAVAILABLE.getStatusCode());

  public static final int ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT =
      toErrorCode(SERVICE_UNAVAILABLE.getStatusCode()) + 1;

  public static final int ERROR_CODE_SERVER_NOT_READY =
      toErrorCode(SERVICE_UNAVAILABLE.getStatusCode()) + 2;

  public static final int ERROR_CODE_SERVER_ERROR =
      toErrorCode(INTERNAL_SERVER_ERROR.getStatusCode());

  private final ErrorMessages errorMessages;

  public static int toStatusCode(final int errorCode) {
    return errorCode / HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static int toErrorCode(final int statusCode) {
    return statusCode * HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static Response notReady() {
    return Response
        .status(SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, 10)
        .entity(new KsqlErrorMessage(ERROR_CODE_SERVER_NOT_READY, "Server initializing"))
        .build();
  }

  public static Response accessDenied(final String msg) {
    return Response
        .status(FORBIDDEN)
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN, msg))
        .build();
  }

  private Response constructAccessDeniedFromKafkaResponse(final String errorMessage) {
    return Response
        .status(FORBIDDEN)
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMessage))
        .build();
  }

  private Response constructSchemaRegistryNotConfiguredResponse(final String errorMessage) {
    return Response
        .status(PRECONDITION_REQUIRED)
        .entity(new KsqlErrorMessage(ERROR_CODE_SCHEMA_REGISTRY_UNCOFIGURED, errorMessage))
        .build();
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

  public static Response badStatement(final String msg, final String statementText) {
    return badStatement(msg, statementText, new KsqlEntityList());
  }

  public static Response badStatement(
      final String msg,
      final String statementText,
      final KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlStatementErrorMessage(
            ERROR_CODE_BAD_STATEMENT, msg, statementText, entities))
        .build();
  }

  public static Response badStatement(final Throwable t, final String statementText) {
    return badStatement(t, statementText, new KsqlEntityList());
  }

  public static Response badStatement(
      final Throwable t,
      final String statementText,
      final KsqlEntityList entities) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlStatementErrorMessage(
            ERROR_CODE_BAD_STATEMENT, t, statementText, entities))
        .build();
  }

  public static Response queryEndpoint(final String statementText) {
    return Response
        .status(BAD_REQUEST)
        .entity(new KsqlStatementErrorMessage(
            ERROR_CODE_QUERY_ENDPOINT,
            "The following statement types should be issued to the websocket endpoint '/query':"
                + System.lineSeparator()
                + "\t* PRINT"
                + System.lineSeparator()
                + "\t* SELECT",
            statementText, new KsqlEntityList()))
        .build();
  }

  public static Response notFound(final String msg) {
    return Response
        .status(NOT_FOUND)
        .entity(new KsqlErrorMessage(ERROR_CODE_NOT_FOUND, msg))
        .build();
  }

  public static Response serverErrorForStatement(final Throwable t, final String statementText) {
    return serverErrorForStatement(t, statementText, new KsqlEntityList());
  }

  public static Response serverErrorForStatement(
      final Throwable t, final String statementText, final KsqlEntityList entities) {
    return Response
        .status(INTERNAL_SERVER_ERROR)
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_SERVER_ERROR, t, statementText, entities))
        .build();
  }

  public static Response commandQueueCatchUpTimeout(final long cmdSeqNum) {
    final String errorMsg = "Timed out while waiting for a previous command to execute. "
        + "command sequence number: " + cmdSeqNum;

    return Response
        .status(SERVICE_UNAVAILABLE)
        .entity(new KsqlErrorMessage(ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT, errorMsg))
        .build();
  }

  public static Response serverShuttingDown() {
    return Response
        .status(SERVICE_UNAVAILABLE)
        .entity(new KsqlErrorMessage(
            ERROR_CODE_SERVER_SHUTTING_DOWN,
            "The server is shutting down"))
        .build();
  }

  public static Response serverNotReady(final KsqlErrorMessage error) {
    return Response
        .status(SERVICE_UNAVAILABLE)
        .entity(error)
        .build();
  }
  
  
  public Errors(final ErrorMessages errorMessages) {
    this.errorMessages = Objects.requireNonNull(errorMessages, "errorMessages");
  }

  public Response accessDeniedFromKafkaResponse(final Exception e) {
    return constructAccessDeniedFromKafkaResponse(errorMessages.kafkaAuthorizationErrorMessage(e));
  }

  public Response schemaRegistryNotConfiguredResponse(final Exception e) {
    return constructSchemaRegistryNotConfiguredResponse(
        errorMessages.schemaRegistryUnconfiguredErrorMessage(e));
  }

  public String kafkaAuthorizationErrorMessage(final Exception e) {
    return errorMessages.kafkaAuthorizationErrorMessage(e);
  }

  public String transactionInitTimeoutErrorMessage(final Exception e) {
    return errorMessages.transactionInitTimeoutErrorMessage(e);
  }

  public Response generateResponse(
      final Exception e,
      final Response defaultResponse
  ) {
    if (ExceptionUtils.indexOfType(e, TopicAuthorizationException.class) >= 0) {
      return accessDeniedFromKafkaResponse(e);
    } else if (ExceptionUtils.indexOfType(e, KsqlSchemaRegistryNotConfiguredException.class) >= 0) {
      return schemaRegistryNotConfiguredResponse(e);
    } else {
      return defaultResponse;
    }
  }
}
