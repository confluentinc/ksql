/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.validation;

import io.confluent.ksql.statement.ConfiguredStatement;

/**
 * A class that wraps a {@link ConfiguredStatement} with whether or not
 * its result should be returned to the user. This class can be removed
 * when we deprecate {@link io.confluent.ksql.parser.tree.RunScript} in
 * a future release.
 */
public final class ValidatedStatement {

  private final ConfiguredStatement<?> statement;
  private final boolean shouldReturnResult;

  private ValidatedStatement(
      final ConfiguredStatement<?> statement,
      final boolean shouldReturnResult
  ) {
    this.statement = statement;
    this.shouldReturnResult = shouldReturnResult;
  }

  public ConfiguredStatement<?> getStatement() {
    return statement;
  }

  public boolean shouldReturnResult() {
    return shouldReturnResult;
  }

  public static ValidatedStatement of(final ConfiguredStatement<?> statement) {
    return new ValidatedStatement(statement, true);
  }

  public static ValidatedStatement ignored(final ValidatedStatement other) {
    return new ValidatedStatement(other.statement, false);
  }
}