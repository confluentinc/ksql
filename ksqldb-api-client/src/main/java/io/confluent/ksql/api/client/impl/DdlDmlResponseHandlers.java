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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class DdlDmlResponseHandlers {

  public static final String EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC =
      "The ksqlDB server accepted the statement issued via executeStatement(), but the response "
      + "received is of an unexpected format. ";
  public static final String EXECUTE_STATEMENT_USAGE_DOC = "The executeStatement() method is only "
      + "for 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS "
      + "SELECT' statements. ";

  private DdlDmlResponseHandlers() {
  }

  static void handleExecuteStatementResponse(
      final JsonObject ksqlEntity,
      final CompletableFuture<ExecuteStatementResult> cf
  ) {
    if (!isCommandStatusEntity(ksqlEntity)) {
      handleUnexpectedEntity(ksqlEntity, cf);
      return;
    }

    try {
      final Optional<String> queryId = Optional.ofNullable(
          ksqlEntity.getJsonObject("commandStatus").getString("queryId"));
      cf.complete(new ExecuteStatementResultImpl(queryId));
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + ksqlEntity
      ));
    }
  }

  static RuntimeException handleUnexpectedNumResponseEntities(final int numEntities) {
    if (numEntities == 0) {
      return new KsqlClientException(EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC
          + EXECUTE_STATEMENT_USAGE_DOC);
    }

    throw new IllegalStateException(
        "Unexpected number of entities in server response: " + numEntities);
  }

  private static boolean isCommandStatusEntity(final JsonObject ksqlEntity) {
    return ksqlEntity.getString("commandId") != null
        && ksqlEntity.getJsonObject("commandStatus") != null;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static void handleUnexpectedEntity(
      final JsonObject ksqlEntity,
      final CompletableFuture<ExecuteStatementResult> cf) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (AdminResponseHandlers.isListStreamsResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "Use the listStreams() method instead."));
    } else if (AdminResponseHandlers.isListTablesResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "Use the listTables() method instead."));
    } else if (AdminResponseHandlers.isListTopicsResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "Use the listTopics() method instead."));
    } else if (AdminResponseHandlers.isListQueriesResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "Use the listQueries() method instead."));
    } else if (AdminResponseHandlers.isDescribeSourceResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "'DESCRIBE <STREAM/TABLE>' statements."));
    } else if (AdminResponseHandlers.isDescribeOrListFunctionResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "'DESCRIBE <FUNCTION>' statements or listing functions."));
    } else if (AdminResponseHandlers.isExplainQueryResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "'EXPLAIN <QUERY_ID>' statements."));
    } else if (AdminResponseHandlers.isListPropertiesResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "listing properties."));
    } else if (AdminResponseHandlers.isListTypesResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "listing custom types."));
    } else if (AdminResponseHandlers.isListConnectorsResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "listing connectors."));
    } else if (AdminResponseHandlers.isDescribeConnectorResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "'DESCRIBE <CONNECTOR>' statements."));
    } else if (AdminResponseHandlers.isCreateConnectorResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC + EXECUTE_STATEMENT_USAGE_DOC
              + "The client does not currently support 'CREATE CONNECTOR' statements."));
    } else if (AdminResponseHandlers.isDropConnectorResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC + EXECUTE_STATEMENT_USAGE_DOC
              + "The client does not currently support 'DROP CONNECTOR' statements."));
    } else if (AdminResponseHandlers.isConnectErrorResponse(ksqlEntity)) {
      cf.completeExceptionally(new KsqlClientException(
          EXECUTE_STATEMENT_USAGE_DOC + "The client does not currently support "
              + "statements for creating, dropping, listing, or describing connectors."));
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response type. Response: " + ksqlEntity
      ));
    }
  }

}
