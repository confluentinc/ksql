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

public class KsqlStatementException extends KsqlException {

  private final String sqlStatement;
  private final boolean isProblemWithStatement;
  private final String rawMessage;

  public KsqlStatementException(final String message, final String sqlStatement) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.isProblemWithStatement = true;
  }

  public KsqlStatementException(final String message,
                                final String sqlStatement,
                                final boolean isProblemWithStatement) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.isProblemWithStatement = isProblemWithStatement;
  }

  public KsqlStatementException(
      final String message,
      final String sqlStatement,
      final Throwable cause) {
    super(message, cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.isProblemWithStatement = true;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public String getRawMessage() {
    return rawMessage;
  }

  public boolean isProblemWithStatement() {
    return isProblemWithStatement;
  }

  @Override
  public String toString() {
    return rawMessage;
  }
}
