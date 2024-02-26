/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;

/**
 * An interface that allows to plug-in custom error handling for Connect server errors, such as 403
 * Forbidden or 401 Unauthorized.
 */
public interface ConnectServerErrors {

  /**
   * Handles error response for a create connector request. This method dispatches to specific
   * methods based on error codes.
   *
   * @param statement the executed statement
   * @param response the failed response
   * @return the optional {@link KsqlEntity} that represents server error
   */
  default Optional<KsqlEntity> handle(
      final ConfiguredStatement<? extends Statement> statement,
      final ConnectResponse<?> response) {
    if (response.httpCode() == FORBIDDEN.code()) {
      return handleForbidden(statement, response);
    } else if (response.httpCode() == UNAUTHORIZED.code()) {
      return handleUnauthorized(statement, response);
    } else {
      return handleDefault(statement, response);
    }
  }

  /**
   * This method allows altering error response on 403 Forbidden.
   *
   * @param statement the executed statement
   * @param response the failed response
   * @return the optional {@code KsqlEntity} that represents server error
   */
  Optional<KsqlEntity> handleForbidden(
      ConfiguredStatement<? extends Statement> statement,
      ConnectResponse<?> response);

  /**
   * This method allows altering error response on 401 Unauthorized.
   *
   * @param statement the executed statement
   * @param response the failed response
   * @return the optional {@code KsqlEntity} that represents server error
   */
  Optional<KsqlEntity> handleUnauthorized(
      ConfiguredStatement<? extends Statement> statement,
      ConnectResponse<?> response);

  /**
   * This method is a fall-back for errors that are not handled by the error-specific methods.
   *
   * @param statement the executed statement
   * @param response the failed response
   * @return the optional {@code KsqlEntity} that represents server error
   */
  Optional<KsqlEntity> handleDefault(
      ConfiguredStatement<? extends Statement> statement,
      ConnectResponse<?> response);
}
