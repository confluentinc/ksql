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

import static io.confluent.ksql.util.SandboxUtil.requireSandbox;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.computation.ValidatedCommandFactory;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Wraps an execution context and information about how to validate statements
 * in a way that can generate a validated checksum against a KSQL query without
 * worrying about races that may occur.
 */
public class RequestValidator {

  private final Map<Class<? extends Statement>, StatementValidator<?>> customValidators;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Function<ServiceContext, KsqlExecutionContext> snapshotSupplier;
  private final ValidatedCommandFactory distributedStatementValidator;

  /**
   * @param customValidators        a map describing how to validate each statement of type
   * @param injectorFactory         creates an {@link Injector} to modify the statements
   * @param snapshotSupplier        supplies a snapshot of the current execution state, the
   *                                snapshot returned will be owned by this class and changes
   *                                to the snapshot should not affect the source and vice versa
   */
  public RequestValidator(
      final Map<Class<? extends Statement>, StatementValidator<?>> customValidators,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Function<ServiceContext, KsqlExecutionContext> snapshotSupplier,
      final ValidatedCommandFactory distributedStatementValidator
  ) {
    this.customValidators = requireNonNull(customValidators, "customValidators");
    this.injectorFactory = requireNonNull(injectorFactory, "injectorFactory");
    this.snapshotSupplier = requireNonNull(snapshotSupplier, "snapshotSupplier");
    this.distributedStatementValidator = requireNonNull(
        distributedStatementValidator, "distributedStatementValidator");
  }

  private boolean isVariableSubstitutionEnabled(final SessionProperties sessionProperties,
      final KsqlConfig ksqlConfig) {
    final Object substitutionEnabled = sessionProperties.getMutableScopedProperties()
        .get(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE);

    if (substitutionEnabled != null && substitutionEnabled instanceof Boolean) {
      return (boolean) substitutionEnabled;
    }

    return ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE);
  }

  /**
   * Validates the messages against a snapshot in time of the KSQL engine.
   *
   * @param statements          the list of statements to validate
   * @param sessionProperties   session properties for this validation
   * @param sql                 the sql that generated the list of statements, used for
   *                            generating more useful debugging information
   *
   * @return the number of new persistent queries that would be created by {@code statements}
   * @throws KsqlException if any of the statements cannot be validated, or the number
   *                       of requested statements would cause the execution context
   *                       to exceed the number of persistent queries that it was configured
   *                       to support
   */
  public int validate(
      final ServiceContext serviceContext,
      final List<ParsedStatement> statements,
      final SessionProperties sessionProperties,
      final String sql
  ) {
    requireSandbox(serviceContext);

    final KsqlExecutionContext ctx = requireSandbox(snapshotSupplier.apply(serviceContext));
    final Injector injector = injectorFactory.apply(ctx, serviceContext);
    final KsqlConfig ksqlConfig = ctx.getKsqlConfig();

    int numPersistentQueries = 0;
    for (final ParsedStatement parsed : statements) {
      final PreparedStatement<?> prepared = ctx.prepare(
          parsed,
          (isVariableSubstitutionEnabled(sessionProperties, ksqlConfig)
              ? sessionProperties.getSessionVariables()
              : Collections.emptyMap())
      );
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(prepared,
          SessionConfig.of(ksqlConfig, sessionProperties.getMutableScopedProperties())
      );

      numPersistentQueries +=
          validate(
              serviceContext,
              configured,
              sessionProperties,
              ctx,
              injector
          );

      if (QueryCapacityUtil.exceedsPersistentQueryCapacity(ctx, ksqlConfig)) {
        QueryCapacityUtil.throwTooManyActivePersistentQueriesException(ctx, ksqlConfig, sql);
      }
    }

    return numPersistentQueries;
  }

  /**
   * @return the number of persistent queries that were validated
   *
   * @throws KsqlStatementException if the statement cannot be validated
   */
  @SuppressWarnings("unchecked")
  private <T extends Statement> int validate(
      final ServiceContext serviceContext,
      final ConfiguredStatement<T> configured,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final Injector injector
  ) throws KsqlStatementException  {
    final Statement statement = configured.getStatement();
    final Class<? extends Statement> statementClass = statement.getClass();
    final StatementValidator<T> customValidator = (StatementValidator<T>)
        customValidators.get(statementClass);

    if (customValidator != null) {
      customValidator.validate(
          configured,
          sessionProperties,
          executionContext,
          serviceContext
      );
    } else if (KsqlEngine.isExecutableStatement(configured.getStatement())
        || configured.getStatement() instanceof PauseQuery
        || configured.getStatement() instanceof ResumeQuery
        || configured.getStatement() instanceof TerminateQuery) {
      final ConfiguredStatement<?> statementInjected = injector.inject(configured);
      distributedStatementValidator.create(statementInjected, serviceContext, executionContext);
    } else {
      throw new KsqlStatementException(
          "Do not know how to validate statement of type: " + statementClass
              + " Known types: " + customValidators.keySet(),
          configured.getMaskedStatementText());
    }

    return (statement instanceof CreateAsSelect || statement instanceof InsertInto) ? 1 : 0;
  }

}
