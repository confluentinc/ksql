/*
 * Copyright 2022 Confluent Inc.
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

import static io.confluent.ksql.rest.Errors.assertionFailedError;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.AssertResource;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class AssertExecutor {

  private static final int RETRY_MS = 100;

  static StatementExecutorResponse execute(
      final String statementText,
      final AssertResource statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final BiConsumer<AssertResource, ServiceContext> assertResource,
      final BiFunction<String, AssertResource, KsqlEntity> createSuccessfulEntity
  ) {
    final int timeout = statement.getTimeout().isPresent()
        ? (int) statement.getTimeout().get().toDuration().toMillis()
        : executionContext.getKsqlConfig().getInt(KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS);
    try {
      RetryUtil.retryWithBackoff(
          timeout / RETRY_MS,
          RETRY_MS,
          RETRY_MS,
          () -> assertResource.accept(statement, serviceContext)
      );
    } catch (final KsqlException e) {
      throw new KsqlRestException(assertionFailedError(e.getMessage()));
    }
    return StatementExecutorResponse.handled(Optional.of(createSuccessfulEntity.apply(statementText, statement)));
  }
}
