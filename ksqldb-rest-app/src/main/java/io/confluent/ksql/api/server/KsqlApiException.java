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

package io.confluent.ksql.api.server;

import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

/**
 * Represents an error due to user error that can be propagated to the user. Do not use this for
 * internal errors
 */
public class KsqlApiException extends KsqlException {

  private final int errorCode;
  private final Optional<String> statement;

  public KsqlApiException(final String message, final int errorCode) {
    super(message);
    this.errorCode = errorCode;
    this.statement = Optional.empty();
  }

  public KsqlApiException(final String message, final int errorCode, final String statement) {
    super(message);
    this.errorCode = errorCode;
    this.statement = Optional.of(statement);
  }

  public int getErrorCode() {
    return errorCode;
  }

  public Optional<String> getStatement() {
    return statement;
  }
}
