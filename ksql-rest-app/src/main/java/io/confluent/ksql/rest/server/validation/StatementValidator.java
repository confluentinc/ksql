/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.validation;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;

/**
 * An interface that allows for arbitrary validation code of a prepared statement
 * against a point-in-time execution engine.
 */
@FunctionalInterface
public interface StatementValidator<T extends Statement> {

  /**
   * A statement validator that does nothing.
   */
  StatementValidator<?> NO_VALIDATION = (stmt, ectx, sctx) -> { };

  /**
   * Validates the statement against the given parameters, and throws an exception
   * if the statement cannot be validated.
   *
   * @throws KsqlException if {@code statement} cannot be validated against the
   *                       given parameters
   */
  void validate(
      ConfiguredStatement<T> statement,
      KsqlExecutionContext executionContext,
      ServiceContext serviceContext) throws KsqlException;
}