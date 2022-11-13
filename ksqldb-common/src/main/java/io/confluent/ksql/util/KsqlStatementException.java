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

  public enum Problem {
    STATEMENT,
    REQUEST,
    OTHER;
  }

  private final String sqlStatement;
  private final Problem problem;
  private final String rawMessage;
  private final String unloggedDetails;
  private final String rawUnloggedDetails;

  public KsqlStatementException(final String message, final String sqlStatement) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = Problem.STATEMENT;
    this.rawUnloggedDetails = this.rawMessage;
    this.unloggedDetails = buildMessage(message, sqlStatement);
  }

  public KsqlStatementException(final String message,
                                final String unloggedDetails,
                                final String sqlStatement) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = Problem.STATEMENT;
    this.rawUnloggedDetails = unloggedDetails;
    this.unloggedDetails = buildMessage(unloggedDetails, sqlStatement);
  }

  public KsqlStatementException(final String message,
                                final String sqlStatement,
                                final Problem problem) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = problem;
    this.rawUnloggedDetails = rawMessage;
    this.unloggedDetails = null;
  }

  public KsqlStatementException(final String message,
                                final String unloggedDetails,
                                final String sqlStatement,
                                final Problem problem) {
    super(message);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = problem;
    this.rawUnloggedDetails = unloggedDetails;
    this.unloggedDetails = buildMessage(unloggedDetails, sqlStatement);
  }

  public KsqlStatementException(
      final String message,
      final String sqlStatement,
      final Throwable cause) {
    super(message, cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = Problem.STATEMENT;
    this.rawUnloggedDetails = this.rawMessage;
    this.unloggedDetails = null;
  }

  public KsqlStatementException(
      final String message,
      final String unloggedDetails,
      final String sqlStatement,
      final Throwable cause) {
    super(message, cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.rawUnloggedDetails = unloggedDetails;
    this.unloggedDetails = buildMessage(unloggedDetails, sqlStatement);
    this.problem = Problem.STATEMENT;
  }

  public KsqlStatementException(
      final String message,
      final String sqlStatement,
      final Problem problem,
      final Throwable cause) {
    super(message, cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = problem;
    this.rawUnloggedDetails = this.rawMessage;
    this.unloggedDetails = null;
  }

  public KsqlStatementException(
      final String message,
      final String unloggedDetails,
      final String sqlStatement,
      final Problem problem,
      final Throwable cause) {
    super(message, cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
    this.problem = problem;
    this.rawUnloggedDetails = unloggedDetails;
    this.unloggedDetails =
        problem == Problem.OTHER ? unloggedDetails : buildMessage(unloggedDetails, sqlStatement);
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public String getRawMessage() {
    return rawMessage;
  }

  public Problem getProblem() {
    return problem;
  }

  public String getUnloggedMessage() {
    return unloggedDetails == null ? getMessage() : unloggedDetails;
  }

  public String getRawUnloggedDetails() {
    return rawUnloggedDetails;
  }

  private static String buildMessage(final String message, final String sqlStatement) {
    return message + System.lineSeparator() + "Statement: " + sqlStatement;
  }

  @Override
  public String toString() {
    return rawMessage;
  }
}
