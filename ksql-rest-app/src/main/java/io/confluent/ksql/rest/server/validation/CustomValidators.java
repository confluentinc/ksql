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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.server.execution.DescribeFunctionExecutor;
import io.confluent.ksql.rest.server.execution.ExplainExecutor;
import io.confluent.ksql.rest.server.execution.ListSourceExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A set of {@code StatementValidator}s which are used to validate non-executable
 * statements. Each handles a corresponding {@code Class<? extends Statement>} and
 * is assumed that the {@code PreparedStatement} that is passed in matches the
 * expected class.
 */
public enum CustomValidators implements StatementValidator {

  QUERY_ENDPOINT(Query.class, QueryValidator::validate),
  PRINT_TOPIC(PrintTopic.class, PrintTopicValidator::validate),

  LIST_TOPICS(ListTopics.class, StatementValidator.NO_VALIDATION),
  LIST_REGISTERED_TOPICS(ListRegisteredTopics.class, StatementValidator.NO_VALIDATION),
  LIST_STREAMS(ListStreams.class, StatementValidator.NO_VALIDATION),
  LIST_TABLES(ListTables.class, StatementValidator.NO_VALIDATION),
  LIST_FUNCTIONS(ListFunctions.class, StatementValidator.NO_VALIDATION),
  LIST_QUERIES(ListQueries.class, StatementValidator.NO_VALIDATION),
  LIST_PROPERTIES(ListProperties.class, StatementValidator.NO_VALIDATION),

  SHOW_COLUMNS(ShowColumns.class, ListSourceExecutor::columns),
  EXPLAIN(Explain.class, ExplainExecutor::execute),
  DESCRIBE_FUNCTION(DescribeFunction.class, DescribeFunctionExecutor::execute),
  SET_PROPERTY(SetProperty.class, PropertyValidator::set),
  UNSET_PROPERTY(UnsetProperty.class, PropertyValidator::unset),

  TERMINATE_QUERY(TerminateQuery.class, TerminateQueryValidator::validate);

  public static final Map<Class<? extends Statement>, StatementValidator> VALIDATOR_MAP =
      ImmutableMap.copyOf(
        EnumSet.allOf(CustomValidators.class)
            .stream()
            .collect(Collectors.toMap(
                CustomValidators::getStatementClass,
                Function.identity()))
      );

  private final Class<? extends Statement> statementClass;
  private final StatementValidator validator;

  CustomValidators(
      final Class<? extends Statement> statementClass,
      final StatementValidator validator) {
    this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  private Class<? extends Statement> getStatementClass() {
    return statementClass;
  }

  @Override
  public void validate(
      final PreparedStatement<?> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) throws KsqlException {
    validator.validate(statement, executionContext, serviceContext, ksqlConfig, propertyOverrides);
  }
}