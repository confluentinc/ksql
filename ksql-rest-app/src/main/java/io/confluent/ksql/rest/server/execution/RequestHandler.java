/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Handles prepared statements, resolving side-effects and delegates to any
 * commands to underlying executors.
 */
public class RequestHandler {

  private final Map<Class<? extends Statement>, StatementExecutor> customExecutors;
  private final Predicate<Class<? extends Statement>> mustSynchronize;
  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final DistributingExecutor distributor;
  private final CommandQueue commandQueue;
  private final Duration distributedCommandSyncTimeout;

  /**
   * @param customExecutors a map describing how to execute statements that do not need
   *                        to be distributed onto the command topic
   * @param mustSynchronize a predicate describing which statements must wait for previous
   *                        distributed statements to finish before handling (a value of
   *                        {@code true} will require synchronization)
   * @param distributor     the distributor to use if there is no custom executor for a given
   *                        statement
   * @param ksqlEngine      the primary KSQL engine - the state of this engine <b>will</b>
   *                        be directly modified by this class
   * @param ksqlConfig      a configuration
   * @param serviceContext  a service context
   * @param commandQueue    the command queue to distribute to
   * @param distSyncTimeout the maximum amount of time to wait for previously distributed
   */
  public RequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor> customExecutors,
      final Predicate<Class<? extends Statement>> mustSynchronize,
      final DistributingExecutor distributor,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final CommandQueue commandQueue,
      final Duration distSyncTimeout
  ) {
    this.customExecutors = Objects.requireNonNull(customExecutors, "customExecutors");
    this.mustSynchronize = Objects.requireNonNull(mustSynchronize);
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.serviceContext = Objects.requireNonNull(serviceContext);
    this.distributor = Objects.requireNonNull(distributor);
    this.commandQueue = Objects.requireNonNull(commandQueue);
    this.distributedCommandSyncTimeout = Objects.requireNonNull(distSyncTimeout);
  }

  public KsqlEntityList execute(
      final List<ParsedStatement> statements,
      final Map<String, Object> propertyOverrides
  ) {
    final Map<String, Object> scopedPropertyOverrides = new HashMap<>(propertyOverrides);
    final KsqlEntityList entities = new KsqlEntityList();
    for (ParsedStatement parsed : statements) {
      final PreparedStatement prepared = ksqlEngine.prepare(parsed);
      if (prepared.getStatement() instanceof RunScript) {
        entities.addAll(executeRunScript(prepared, ksqlEngine, propertyOverrides));
      } else {
        executeStatement(prepared, scopedPropertyOverrides, entities).ifPresent(entities::add);
      }
    }
    return entities;
  }

  private Optional<KsqlEntity> executeStatement(
      final PreparedStatement<?> prepared,
      final Map<String, Object> propertyOverrides,
      final KsqlEntityList entities
  ) {
    final Class<? extends Statement> statementClass = prepared.getStatement().getClass();
    maybeWaitForPreviousCommands(statementClass, entities);

    final StatementExecutor executor = customExecutors.getOrDefault(statementClass, distributor);
    return executor.execute(
        prepared,
        ksqlEngine,
        serviceContext,
        ksqlConfig,
        propertyOverrides);
  }

  private KsqlEntityList executeRunScript(
      final PreparedStatement<?> statement,
      final KsqlEngine ksqlEngine,
      final Map<String, Object> propertyOverrides) {
    final String sql = (String) propertyOverrides
        .get(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT);

    if (sql == null) {
      throw new KsqlStatementException(
          "Request is missing script content", statement.getStatementText());
    }

    return execute(ksqlEngine.parse(sql), propertyOverrides);
  }

  private void maybeWaitForPreviousCommands(
      final Class<? extends Statement> statementClass,
      final KsqlEntityList entities
  ) {
    if (mustSynchronize.test(statementClass)) {
      final ArrayList<KsqlEntity> reversed = new ArrayList<>(entities);
      Collections.reverse(reversed);

      reversed.stream()
          .filter(e -> e instanceof CommandStatusEntity)
          .map(CommandStatusEntity.class::cast)
          .map(CommandStatusEntity::getCommandSequenceNumber)
          .findFirst()
          .ifPresent(seqNum -> {
            try {
              commandQueue.ensureConsumedPast(seqNum, distributedCommandSyncTimeout);
            } catch (final InterruptedException e) {
              throw new KsqlRestException(Errors.serverShuttingDown());
            } catch (final TimeoutException e) {
              throw new KsqlRestException(Errors.commandQueueCatchUpTimeout(seqNum));
            }
          });
    }
  }

}
