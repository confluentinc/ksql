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

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.server.validation.ValidatedStatement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlServerException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Handles prepared statements, resolving side-effects and delegates to any
 * commands to underlying executors.
 */
public class RequestHandler {

  private final Map<Class<? extends Statement>, StatementExecutor<?>> customExecutors;
  private final KsqlEngine ksqlEngine;
  private final DistributingExecutor distributor;
  private final CommandQueueSync commandQueueSync;

  /**
   * @param customExecutors a map describing how to execute statements that do not need
   *                        to be distributed onto the command topic
   * @param distributor     the distributor to use if there is no custom executor for a given
   *                        statement
   * @param ksqlEngine      the primary KSQL engine - the state of this engine <b>will</b>
   *                        be directly modified by this class
   */
  public RequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor<?>> customExecutors,
      final DistributingExecutor distributor,
      final KsqlEngine ksqlEngine,
      final CommandQueueSync commandQueueSync
  ) {
    this.customExecutors = Objects.requireNonNull(customExecutors, "customExecutors");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.distributor = Objects.requireNonNull(distributor, "distributor");
    this.commandQueueSync = Objects.requireNonNull(commandQueueSync, "commandQueueSync");
  }

  public KsqlEntityList execute(
      final ServiceContext serviceContext,
      final List<ValidatedStatement> statements
  ) {
    final KsqlEntityList entities = new KsqlEntityList();

    for (ValidatedStatement statement : statements) {
      final Optional<KsqlEntity> result =
          executeStatement(serviceContext, statement.getStatement(), entities);

      if (statement.shouldReturnResult()) {
        result.ifPresent(entities::add);
      }
    }

    return entities;
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> Optional<KsqlEntity> executeStatement(
      final ServiceContext serviceContext,
      final ConfiguredStatement<T> configured,
      final KsqlEntityList entities
  ) {
    final Class<? extends Statement> statementClass = configured.getStatement().getClass();
    if (RunScript.class.isAssignableFrom(statementClass)) {
      throw new KsqlServerException(
          "Expected RunScript to be unfolded in RequestValidator: " + configured);
    }

    commandQueueSync.waitFor(new KsqlEntityList(entities), statementClass);

    final StatementExecutor<T> executor = (StatementExecutor<T>)
        customExecutors.getOrDefault(statementClass, distributor);

    return executor.execute(
        configured,
        ksqlEngine,
        serviceContext
    );
  }

}
