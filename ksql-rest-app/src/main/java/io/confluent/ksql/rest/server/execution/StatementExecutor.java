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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;

/**
 * An interface that allows for arbitrary execution code of a prepared statement.
 */
@FunctionalInterface
public interface StatementExecutor {

  /**
   * Executes the query against the parameterized {@code ksqlEngine}.
   *
   * @return the execution result, if present, else {@link Optional#empty()}
   */
  Optional<KsqlEntity> execute(
      PreparedStatement<?> statement,
      KsqlExecutionContext executionContext,
      ServiceContext serviceContext,
      KsqlConfig ksqlConfig,
      Map<String, Object> propertyOverrides);

}
