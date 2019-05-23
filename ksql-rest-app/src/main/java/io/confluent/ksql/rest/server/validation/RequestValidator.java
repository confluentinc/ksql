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

import com.google.common.collect.Iterables;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.TopicAccessValidator;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.Checksum;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps an execution context and information about how to validate statements
 * in a way that can generate a validated checksum against a KSQL query without
 * worrying about races that may occur.
 */
public class RequestValidator {

  private static final Logger LOG = LoggerFactory.getLogger(RequestValidator.class);

  private final Map<Class<? extends Statement>, StatementValidator<?>> customValidators;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final KsqlConfig ksqlConfig;
  private final TopicAccessValidator topicAccessValidator;

  /**
   * @param customValidators        a map describing how to validate each statement of type
   * @param injectorFactory         creates an {@link Injector} to modify the statements
   * @param ksqlConfig              the {@link KsqlConfig} to validate against
   */
  public RequestValidator(
      final Map<Class<? extends Statement>, StatementValidator<?>> customValidators,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final KsqlConfig ksqlConfig,
      final TopicAccessValidator topicAccessValidator
  ) {
    this.customValidators = requireNonNull(customValidators, "customValidators");
    this.injectorFactory = requireNonNull(injectorFactory, "injectorFactory");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.topicAccessValidator = topicAccessValidator;
  }

  /**
   * Validates the messages against a snapshot in time of the KSQL engine.
   *
   * @param ctx                 a sandbox execution context to validate against
   * @param statements          the list of statements to validate
   * @param propertyOverrides   a map of properties to override for this validation
   * @param sql                 the sql that generated the list of statements, used for
   *                            generating more useful debugging information
   *
   * @return a list of {@link ValidatedStatement} that represent the result of validation
   * @throws KsqlException if any of the statements cannot be validated, or the number
   *                       of requested statements would cause the execution context
   *                       to exceed the number of persistent queries that it was configured
   *                       to support
   */
  public List<ValidatedStatement> validate(
      final KsqlExecutionContext ctx,
      final ServiceContext serviceContext,
      final List<ParsedStatement> statements,
      final Map<String, Object> propertyOverrides,
      final String sql
  ) {
    requireSandbox(serviceContext);
    requireSandbox(ctx);

    final Injector injector = injectorFactory.apply(ctx, serviceContext);

    final List<ValidatedStatement> validatedStatements = new ArrayList<>();

    for (ParsedStatement parsed : statements) {
      final PreparedStatement<?> prepared = ctx.prepare(parsed);
      final Checksum checksum = ctx.getChecksum();
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(
          prepared, propertyOverrides, ksqlConfig, checksum);

      if (prepared.getStatement() instanceof RunScript) {
        validatedStatements.addAll(validateRunScript(serviceContext, configured, ctx));
      } else {
        validatedStatements.add(validate(serviceContext, configured, ctx, injector));
      }
    }

    final int numPersistentQueries =
        (int) validatedStatements.stream()
            .map(ValidatedStatement::getStatement)
            .map(ConfiguredStatement::getStatement)
            .filter(s -> s instanceof CreateAsSelect || s instanceof InsertInto)
            .count();
    if (QueryCapacityUtil.exceedsPersistentQueryCapacity(ctx, ksqlConfig, numPersistentQueries)) {
      QueryCapacityUtil.throwTooManyActivePersistentQueriesException(ctx, ksqlConfig, sql);
    }

    return validatedStatements;
  }

  /**
   * @return the number of persistent queries that were validated
   *
   * @throws KsqlStatementException if the statement cannot be validated
   */
  @SuppressWarnings("unchecked")
  private <T extends Statement> ValidatedStatement validate(
      final ServiceContext serviceContext,
      final ConfiguredStatement<T> configured,
      final KsqlExecutionContext executionContext,
      final Injector injector
  ) throws KsqlStatementException  {
    final Statement statement = configured.getStatement();
    final Class<? extends Statement> statementClass = statement.getClass();
    final StatementValidator<T> customValidator = (StatementValidator<T>)
        customValidators.get(statementClass);

    if (customValidator != null) {
      customValidator.validate(configured, executionContext, serviceContext);
    } else if (KsqlEngine.isExecutableStatement(configured.getStatement())) {
      final ConfiguredStatement<?> statementInjected = injector.inject(configured);

      topicAccessValidator.validate(
          serviceContext,
          executionContext.getMetaStore(),
          statementInjected.getStatement()
      );

      executionContext.execute(serviceContext, statementInjected);
    } else {
      throw new KsqlStatementException(
          "Do not know how to validate statement of type: " + statementClass
              + " Known types: " + customValidators.keySet(),
          configured.getStatementText());
    }

    return ValidatedStatement.of(configured);
  }

  private List<ValidatedStatement> validateRunScript(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext executionContext) {
    final String sql = (String) statement.getOverrides()
        .get(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT);

    if (sql == null) {
      throw new KsqlStatementException(
          "Request is missing script content", statement.getStatementText());
    }

    LOG.warn("RUN SCRIPT statement detected. "
        + "Note: RUN SCRIPT is deprecated and will be removed in the next major version. "
        + "statement: " + statement.getStatementText());

    final List<ValidatedStatement> validatedStatements =
        validate(
            executionContext,
            serviceContext,
            executionContext.parse(sql),
            statement.getOverrides(),
            sql);

    if (validatedStatements.isEmpty()) {
      return validatedStatements;
    }

    // This is to maintain backwards compatibility until we deprecate
    // RunScript in the next major release - the expected behavior was
    // to return only the last entity
    final List<ValidatedStatement> results =
        validatedStatements.subList(0, validatedStatements.size() - 1)
            .stream()
            .map(ValidatedStatement::ignored)
            .collect(Collectors.toCollection(ArrayList::new));
    results.add(Iterables.getLast(validatedStatements));
    return results;
  }

}
