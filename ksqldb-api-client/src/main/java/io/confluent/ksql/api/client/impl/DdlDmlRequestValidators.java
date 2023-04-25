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
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

final class DdlDmlRequestValidators {

  /**
   * This regex is used to split the statement by quoted strings/identifier pieces.
   * <p>
   * The pattern does not have catastrophic backtracking, but leaves open the possibility that a
   * single quoted string/identifier will be split into multiple pieces, which is fine for counting
   * the number of statements in {@link #countStatements(String)}. However, it would not be fine
   * for other types of potential use cases that require identifying quoted strings/identifiers
   * (e.g., validating whether an identifier name exists, checking for max length of identifiers,
   * etc).
   * </p>
   * <p>
   *   See git blame and <a href="https://github.com/confluentinc/ksql/pull/9855">
   *     https://github.com/confluentinc/ksql/pull/985</a> for more details.
   * </p>
   **/
  private static final String QUOTED_STRING_OR_IDENTIFIER_PIECE = "(`[^`]*`)|('[^']*')";

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

    if (countStatements(sql) > 1) {
      cf.completeExceptionally(new KsqlClientException(
          "executeStatement() may only be used to execute one statement at a time."));
      return false;
    }

    return true;
  }

  /**
   * Counts the number of sql statements in a string by
   *  1. Removing all of the sql strings and identifiers
   *  2. Splitting the remaining substrings by ';'. The -1 argument in the split
   *     function call ensures that each ';' will always have two partitions surrounding it, so that
   *     the number of partitions is the same whether or not the final ';' has whitespace after it.
   *  3. Counting the partitions
   * @param sql a string containing sql statements
   * @return the number of sql statements in the string
   */
  private static int countStatements(final String sql) {
    return Arrays.stream(sql.split(QUOTED_STRING_OR_IDENTIFIER_PIECE))
        .mapToInt(part -> part.split(";", -1).length - 1)
        .sum();
  }
}
