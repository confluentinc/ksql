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

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
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

  private boolean isVariableSubstitutionEnabled(final SessionProperties sessionProperties) {
    final Object substitutionEnabled = sessionProperties.getMutableScopedProperties()
        .get(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE);

    if (substitutionEnabled != null && substitutionEnabled instanceof Boolean) {
      return (boolean) substitutionEnabled;
    }

    return ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE);
  }

  public KsqlEntityList execute(
      final KsqlSecurityContext securityContext,
      final List<ParsedStatement> statements,
      final SessionProperties sessionProperties
  ) {
    final KsqlEntityList entities = new KsqlEntityList();
    for (final ParsedStatement parsed : statements) {
      final PreparedStatement<?> prepared = ksqlEngine.prepare(
          parsed,
          (isVariableSubstitutionEnabled(sessionProperties)
              ? sessionProperties.getSessionVariables()
              : Collections.emptyMap())
      );
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(prepared,
          SessionConfig.of(ksqlConfig, sessionProperties.getMutableScopedProperties())
      );

      executeStatement(
          securityContext,
          configured,
          sessionProperties,
          entities
      ).ifPresent(entities::add);
    }
    return entities;
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> Optional<KsqlEntity> executeStatement(
      final KsqlSecurityContext securityContext,
      final ConfiguredStatement<T> configured,
      final SessionProperties sessionProperties,
      final KsqlEntityList entities
  ) {
    final Class<? extends Statement> statementClass = configured.getStatement().getClass();
    
    commandQueueSync.waitFor(new KsqlEntityList(entities), statementClass);

    final StatementExecutor<T> executor = (StatementExecutor<T>) customExecutors.getOrDefault(
        statementClass,
        (stmt, props, ctx, svcCtx) -> distributor.execute(stmt, ctx, securityContext)
    );

    return executor.execute(
        configured,
        sessionProperties,
        ksqlEngine,
        securityContext.getServiceContext()
    );
  }

}
