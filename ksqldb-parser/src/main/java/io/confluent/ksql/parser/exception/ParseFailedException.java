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

package io.confluent.ksql.parser.exception;

import io.confluent.ksql.util.KsqlStatementException;

public class ParseFailedException extends KsqlStatementException {

  public ParseFailedException(final String message, final String sqlStatement) {
    super(message, sqlStatement);
  }

  public ParseFailedException(final String message,
                              final String unloggedDetails,
                              final String sqlStatement) {
    super(message, unloggedDetails, sqlStatement);
  }

  public ParseFailedException(
      final String message,
      final String sqlStatement,
      final Throwable cause) {
    super(message, sqlStatement, cause);
  }

  public ParseFailedException(
      final String message,
      final String unloggedDetails,
      final String sqlStatement,
      final Throwable cause) {
    super(message, unloggedDetails, sqlStatement, cause);
  }
}