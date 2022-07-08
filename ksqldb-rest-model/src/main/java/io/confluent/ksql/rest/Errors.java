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

import static io.netty.handler.codec.http.HttpHeaderNames.RETRY_AFTER;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.MISDIRECTED_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_REQUIRED;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.util.KsqlSchemaRegistryNotConfiguredException;
import java.util.Objects;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.TopicAuthorizationException;

public final class Errors {

  private static final int HTTP_TO_ERROR_CODE_MULTIPLIER = 100;

  public static final int ERROR_CODE_MISDIRECTED_REQUEST = toErrorCode(MISDIRECTED_REQUEST.code());

  public static final int ERROR_CODE_BAD_REQUEST = toErrorCode(BAD_REQUEST.code());
  public static final int ERROR_CODE_BAD_STATEMENT = toErrorCode(BAD_REQUEST.code()) + 1;
  private static final int ERROR_CODE_QUERY_ENDPOINT = toErrorCode(BAD_REQUEST.code()) + 2;

  public static final int ERROR_CODE_MAX_PUSH_QUERIES_EXCEEDED =
      toErrorCode(BAD_REQUEST.code()) + 3;

  public static final int ERROR_CODE_HTTP2_ONLY = toErrorCode(BAD_REQUEST.code()) + 4;
  public static final int ERROR_CODE_INTERNAL_ONLY = toErrorCode(BAD_REQUEST.code()) + 5;

  public static final int ERROR_CODE_UNAUTHORIZED = toErrorCode(UNAUTHORIZED.code());

  public static final int ERROR_CODE_FORBIDDEN = toErrorCode(FORBIDDEN.code());
  public static final int ERROR_CODE_FORBIDDEN_KAFKA_ACCESS =
      toErrorCode(FORBIDDEN.code()) + 1;

  public static final int ERROR_CODE_SCHEMA_REGISTRY_UNCONFIGURED =
      toErrorCode(PRECONDITION_REQUIRED.code()) + 1;

  public static final int ERROR_CODE_NOT_FOUND = toErrorCode(NOT_FOUND.code());

  public static final int ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT =
      toErrorCode(SERVICE_UNAVAILABLE.code()) + 1;

  public static final int ERROR_CODE_SERVER_NOT_READY =
      toErrorCode(SERVICE_UNAVAILABLE.code()) + 2;

  public static final int ERROR_CODE_SERVER_SHUTTING_DOWN =
      toErrorCode(SERVICE_UNAVAILABLE.code()) + 3;

  public static final int ERROR_CODE_SERVER_ERROR =
      toErrorCode(INTERNAL_SERVER_ERROR.code());
  
  public static final int ERROR_CODE_TOO_MANY_REQUESTS = toErrorCode(TOO_MANY_REQUESTS.code());

  private final ErrorMessages errorMessages;

  public static int toStatusCode(final int errorCode) {
    return errorCode / HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static int toErrorCode(final int statusCode) {
    return statusCode * HTTP_TO_ERROR_CODE_MULTIPLIER;
  }

  public static EndpointResponse notReady() {
    return EndpointResponse.create()
        .status(SERVICE_UNAVAILABLE.code())
        .header(RETRY_AFTER.toString(), 10)
        .entity(new KsqlErrorMessage(ERROR_CODE_SERVER_NOT_READY, "Server initializing"))
        .build();
  }

  private EndpointResponse constructAccessDeniedFromKafkaResponse(final String errorMessage) {
    return EndpointResponse.create()
        .status(FORBIDDEN.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMessage))
        .build();
  }

  private EndpointResponse constructSchemaRegistryNotConfiguredResponse(final String errorMessage) {
    return EndpointResponse.create()
        .status(PRECONDITION_REQUIRED.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_SCHEMA_REGISTRY_UNCONFIGURED, errorMessage))
        .build();
  }

  public static EndpointResponse badRequest(final String msg) {
    return EndpointResponse.create()
        .status(BAD_REQUEST.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, msg))
        .build();
  }

  public static EndpointResponse badRequest(final Throwable t) {
    return EndpointResponse.create()
        .status(BAD_REQUEST.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_BAD_REQUEST, t))
        .build();
  }

  public static EndpointResponse badStatement(final String msg, final String statementText) {
    return badStatement(msg, statementText, new KsqlEntityList());
  }

  public static EndpointResponse badStatement(
      final String msg,
      final String statementText,
      final KsqlEntityList entities) {
    return EndpointResponse.create()
        .status(BAD_REQUEST.code())
        .entity(new KsqlStatementErrorMessage(
            ERROR_CODE_BAD_STATEMENT, msg, statementText, entities))
        .build();
  }

  public static EndpointResponse badStatement(final Throwable t, final String statementText) {
    return badStatement(t, statementText, new KsqlEntityList());
  }

  public static EndpointResponse badStatement(
      final Throwable t,
      final String statementText,
      final KsqlEntityList entities) {
    return EndpointResponse.create()
        .status(BAD_REQUEST.code())
        .entity(new KsqlStatementErrorMessage(
            ERROR_CODE_BAD_STATEMENT, t, statementText, entities))
        .build();
  }

  public static EndpointResponse queryEndpoint(final String statementText) {
    return EndpointResponse.create()
        .status(BAD_REQUEST.code())
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

  public static EndpointResponse notFound(final String msg) {
    return EndpointResponse.create()
        .status(NOT_FOUND.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_NOT_FOUND, msg))
        .build();
  }

  public static EndpointResponse serverErrorForStatement(final Throwable t,
      final String statementText) {
    return serverErrorForStatement(t, statementText, new KsqlEntityList());
  }

  public static EndpointResponse serverErrorForStatement(
      final Throwable t, final String statementText, final KsqlEntityList entities) {
    return EndpointResponse.create()
        .status(INTERNAL_SERVER_ERROR.code())
        .entity(new KsqlStatementErrorMessage(ERROR_CODE_SERVER_ERROR, t, statementText, entities))
        .build();
  }

  public static EndpointResponse commandQueueCatchUpTimeout(final long cmdSeqNum) {
    final String errorMsg = "Timed out while waiting for a previous command to execute. "
        + "command sequence number: " + cmdSeqNum;

    return EndpointResponse.create()
        .status(SERVICE_UNAVAILABLE.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT, errorMsg))
        .build();
  }

  public static EndpointResponse serverShuttingDown() {
    return EndpointResponse.create()
        .status(SERVICE_UNAVAILABLE.code())
        .entity(new KsqlErrorMessage(
            ERROR_CODE_SERVER_SHUTTING_DOWN,
            "The server is shutting down"))
        .build();
  }

  public static EndpointResponse serverNotReady(final KsqlErrorMessage error) {
    return EndpointResponse.create()
        .status(SERVICE_UNAVAILABLE.code())
        .entity(error)
        .build();
  }

  public static EndpointResponse tooManyRequests(final String msg) {
    return EndpointResponse.create()
      .status(TOO_MANY_REQUESTS.code())
      .entity(new KsqlErrorMessage(ERROR_CODE_TOO_MANY_REQUESTS, msg))
      .build();
  }

  public Errors(final ErrorMessages errorMessages) {
    this.errorMessages = Objects.requireNonNull(errorMessages, "errorMessages");
  }

  public EndpointResponse accessDeniedFromKafkaResponse(final Exception e) {
    return constructAccessDeniedFromKafkaResponse(errorMessages.kafkaAuthorizationErrorMessage(e));
  }

  public EndpointResponse schemaRegistryNotConfiguredResponse(final Exception e) {
    return constructSchemaRegistryNotConfiguredResponse(
        errorMessages.schemaRegistryUnconfiguredErrorMessage(e));
  }

  public String kafkaAuthorizationErrorMessage(final Exception e) {
    return errorMessages.kafkaAuthorizationErrorMessage(e);
  }

  public String transactionInitTimeoutErrorMessage(final Exception e) {
    return errorMessages.transactionInitTimeoutErrorMessage(e);
  }

  public String commandRunnerDegradedIncompatibleCommandsErrorMessage() {
    return errorMessages.commandRunnerDegradedIncompatibleCommandsErrorMessage();
  }

  public String commandRunnerDegradedCorruptedErrorMessage() {
    return errorMessages.commandRunnerDegradedCorruptedErrorMessage();
  }

  public EndpointResponse generateResponse(
      final Exception e,
      final EndpointResponse defaultResponse
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
