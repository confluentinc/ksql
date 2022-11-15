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
import java.util.concurrent.CompletableFuture;

final class DdlDmlRequestValidators {

  private DdlDmlRequestValidators() {
  }

  static boolean validateExecuteStatementRequest(
      final String sql,
      final CompletableFuture<ExecuteStatementResult> cf
  ) {
    if (!sql.contains(";")) {
      cf.completeExceptionally(new KsqlClientException(
          "Missing semicolon in SQL for executeStatement() request."));
      return false;
    }

    if (sql.indexOf(";") != sql.lastIndexOf(";")) {
      cf.completeExceptionally(new KsqlClientException(
          "executeStatement() may only be used to execute one statement at a time."));
      return false;
    }

    return true;
  }
}
