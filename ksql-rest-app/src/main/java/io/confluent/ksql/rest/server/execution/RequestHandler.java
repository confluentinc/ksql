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

import com.google.common.collect.Iterables;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.TransactionalProducer;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
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
  private final KsqlConfig ksqlConfig;
  private final DistributingExecutor distributor;
  private final CommandQueueSync commandQueueSync;

  /**
   * @param customExecutors a map describing how to execute statements that do not need
   *                        to be distributed onto the command topic
   * @param distributor     the distributor to use if there is no custom executor for a given
   *                        statement
   * @param ksqlEngine      the primary KSQL engine - the state of this engine <b>will</b>
   *                        be directly modified by this class
   * @param ksqlConfig      a configuration
   */
  public RequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor<?>> customExecutors,
      final DistributingExecutor distributor,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final CommandQueueSync commandQueueSync
  ) {
    this.customExecutors = Objects.requireNonNull(customExecutors, "customExecutors");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.distributor = Objects.requireNonNull(distributor, "distributor");
    this.commandQueueSync = Objects.requireNonNull(commandQueueSync, "commandQueueSync");
  }

  public KsqlEntityList execute(
      final ServiceContext serviceContext,
      final List<ParsedStatement> statements,
      final Map<String, Object> propertyOverrides,
      final TransactionalProducer transactionalProducer
  ) {
    final Map<String, Object> scopedPropertyOverrides = new HashMap<>(propertyOverrides);
    final KsqlEntityList entities = new KsqlEntityList();
    for (ParsedStatement parsed : statements) {
      final PreparedStatement<?> prepared = ksqlEngine.prepare(parsed);
      if (prepared.getStatement() instanceof RunScript) {
        final KsqlEntityList result = executeRunScript(
            serviceContext,
            prepared,
            propertyOverrides,
                transactionalProducer
        );
        if (!result.isEmpty()) {
          // This is to maintain backwards compatibility until we deprecate
          // RunScript in the next major release - the expected behavior was
          // to return only the last entity
          entities.add(Iterables.getLast(result));
        }
      } else {
        final ConfiguredStatement<?> configured = ConfiguredStatement.of(
            prepared, scopedPropertyOverrides, ksqlConfig);
        executeStatement(
            serviceContext,
            configured,
            scopedPropertyOverrides,
            entities,
                transactionalProducer
        ).ifPresent(entities::add);
      }
    }
    return entities;
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> Optional<KsqlEntity> executeStatement(
      final ServiceContext serviceContext,
      final ConfiguredStatement<T> configured,
      final Map<String, Object> mutableScopedProperties,
      final KsqlEntityList entities,
      final TransactionalProducer transactionalProducer
  ) {
    final Class<? extends Statement> statementClass = configured.getStatement().getClass();
    commandQueueSync.waitFor(new KsqlEntityList(entities), statementClass);

    final StatementExecutor<T> executor = (StatementExecutor<T>)
        customExecutors.getOrDefault(statementClass, distributor);

    if (executor instanceof DistributingExecutor) {
      ((DistributingExecutor) executor).setTransactionManager(transactionalProducer);
    }

    return executor.execute(
        configured,
        mutableScopedProperties,
        ksqlEngine,
        serviceContext
    );
  }

  private KsqlEntityList executeRunScript(
      final ServiceContext serviceContext,
      final PreparedStatement<?> statement,
      final Map<String, Object> propertyOverrides,
      final TransactionalProducer transactionalProducer
  ) {
    final String sql = (String) propertyOverrides
        .get(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT);

    if (sql == null) {
      throw new KsqlStatementException(
          "Request is missing script content", statement.getStatementText());
    }

    return execute(
        serviceContext,
        ksqlEngine.parse(sql),
        propertyOverrides,
            transactionalProducer
    );
  }
}
