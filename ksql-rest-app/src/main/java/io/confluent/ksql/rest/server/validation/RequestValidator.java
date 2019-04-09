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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.client.properties.LocalPropertyValidator;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
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
  private final Function<KsqlExecutionContext, Injector> topicInjectorFactory;
  private final Function<ServiceContext, Injector> schemaInjectorFactory;
  private final Supplier<KsqlExecutionContext> snapshotSupplier;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;

  /**
   * @param customValidators        a map describing how to validate each statement of type
   * @param schemaInjectorFactory   creates a {@link SchemaInjector} for schema validation
   * @param snapshotSupplier        supplies a snapshot of the current execution state, the
   *                                snapshot returned will be owned by this class and changes
   *                                to the snapshot should not affect the source and vice versa
   * @param serviceContext          the {@link ServiceContext} to use
   * @param ksqlConfig              the {@link KsqlConfig} to validate against
   */
  public RequestValidator(
      final Map<Class<? extends Statement>, StatementValidator<?>> customValidators,
      final Function<ServiceContext, Injector> schemaInjectorFactory,
      final Function<KsqlExecutionContext, Injector> topicInjectorFactory,
      final Supplier<KsqlExecutionContext> snapshotSupplier,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig
  ) {
    this.customValidators = requireNonNull(customValidators, "customValidators");
    this.schemaInjectorFactory = requireNonNull(schemaInjectorFactory, "schemaInjector");
    this.topicInjectorFactory = requireNonNull(topicInjectorFactory, "topicInjectorFactory");
    this.snapshotSupplier = requireNonNull(snapshotSupplier, "snapshotSupplier");
    this.serviceContext = requireSandbox(requireNonNull(serviceContext, "serviceContext"));
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
  }

  /**
   * Validates the messages against a snapshot in time of the KSQL engine.
   *
   * @param statements          the list of statements to validate
   * @param propertyOverrides   a map of properties to override for this validation
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
      final List<ParsedStatement> statements,
      final Map<String, Object> propertyOverrides,
      final String sql
  ) {
    validateOverriddenConfigProperties(propertyOverrides);
    final KsqlExecutionContext ctx = requireSandbox(snapshotSupplier.get());
    final Injector schemaInjector = schemaInjectorFactory.apply(serviceContext);
    final Injector topicInjector = topicInjectorFactory.apply(ctx);

    int numPersistentQueries = 0;
    for (ParsedStatement parsed : statements) {
      final PreparedStatement<?> prepared = ctx.prepare(parsed);
      final ConfiguredStatement<?> configured = ConfiguredStatement.of(
          prepared, propertyOverrides, ksqlConfig);

      numPersistentQueries += (prepared.getStatement() instanceof RunScript)
          ? validateRunScript(configured, ctx)
          : validate(configured, ctx, schemaInjector, topicInjector);
    }

    if (QueryCapacityUtil.exceedsPersistentQueryCapacity(ctx, ksqlConfig, numPersistentQueries)) {
      QueryCapacityUtil.throwTooManyActivePersistentQueriesException(ctx, ksqlConfig, sql);
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
      final ConfiguredStatement<T> configured,
      final KsqlExecutionContext executionContext,
      final Injector schemaInjector,
      final Injector topicInjector) throws KsqlStatementException  {
    final Statement statement = configured.getStatement();
    final Class<? extends Statement> statementClass = statement.getClass();
    final StatementValidator<T> customValidator = (StatementValidator<T>)
        customValidators.get(statementClass);

    if (customValidator != null) {
      customValidator.validate(configured, executionContext, serviceContext);
    } else if (KsqlEngine.isExecutableStatement(configured.getStatement())) {
      executionContext.execute(topicInjector.inject(schemaInjector.inject(configured)));
    } else {
      throw new KsqlStatementException(
          "Do not know how to validate statement of type: " + statementClass
              + " Known types: " + customValidators.keySet(),
          configured.getStatementText());
    }

    return (statement instanceof CreateAsSelect || statement instanceof InsertInto) ? 1 : 0;
  }

  private int validateRunScript(
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

    return validate(executionContext.parse(sql), statement.getOverrides(), sql);
  }

  private static void validateOverriddenConfigProperties(
      final Map<String, Object> propertyOverrides
  ) {
    propertyOverrides.keySet()
        .forEach(
            propertyName -> {
              if (!LocalPropertyValidator.CONFIG_PROPERTY_WHITELIST.contains(propertyName)) {
                throw new KsqlException("Invalid config property: " + propertyName);
              }
            });
  }

}
