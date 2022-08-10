/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import static io.confluent.ksql.statement.MaskedStatement.EMPTY_MASKED_STATEMENT;

import io.confluent.ksql.statement.MaskedStatement;

public class KsqlStatementException extends KsqlException {

  private final MaskedStatement sqlStatement;
  private final String rawMessage;

  public KsqlStatementException(final String message, final MaskedStatement sqlStatement) {
    super(buildMessage(message, sqlStatement));
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? EMPTY_MASKED_STATEMENT : sqlStatement;
  }

  public KsqlStatementException(
      final String message,
      final MaskedStatement sqlStatement,
      final Throwable cause) {
    super(buildMessage(message, sqlStatement), cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? EMPTY_MASKED_STATEMENT : sqlStatement;
  }

  public MaskedStatement getSqlStatement() {
    return sqlStatement;
  }

  public String getRawMessage() {
    return rawMessage;
  }

  private static String buildMessage(final String message, final MaskedStatement sqlStatement) {
    return message + System.lineSeparator() + "Statement: " + sqlStatement;
  }
}
