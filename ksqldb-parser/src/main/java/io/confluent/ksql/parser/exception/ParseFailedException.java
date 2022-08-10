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

import io.confluent.ksql.statement.MaskedStatement;
import io.confluent.ksql.statement.UnMaskedStatement;
import io.confluent.ksql.util.KsqlStatementException;

public class ParseFailedException extends KsqlStatementException {

  public ParseFailedException(final String message) {
    super(message, MaskedStatement.EMPTY_MASKED_STATEMENT);
  }

  public ParseFailedException(final String message, final UnMaskedStatement sqlStatement) {
    // Statement parsing failure -- just treat it as MaskedStatement
    // Maybe warn? Or best effort Mask?
    super(message, MaskedStatement.of(sqlStatement.toString()));
  }

  public ParseFailedException(
      final String message,
      final UnMaskedStatement sqlStatement,
      final Throwable cause) {
    // Statement parsing failure -- just treat it as MaskedStatement
    // Maybe warn? Or best effort Mask?
    super(message, MaskedStatement.of(sqlStatement.toString()), cause);
  }

  public ParseFailedException(
      final String message,
      final MaskedStatement sqlStatement,
      final Throwable cause) {
    super(message, sqlStatement, cause);
  }
}