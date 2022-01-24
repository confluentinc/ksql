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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.EvaluateExpression;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ExpressionEvaluationEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Optional;

public final class EvaluateExpressionExecutor {

  private EvaluateExpressionExecutor() { }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<EvaluateExpression> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    // Calculate the return type
    final LogicalSchema noColumns = LogicalSchema.builder().build();
    final Expression expression = statement.getStatement().getExpression();
    final ExpressionTypeManager expressionTypeManager
        = new ExpressionTypeManager(noColumns, executionContext.getMetaStore());
    final SqlType returnType = expressionTypeManager.getExpressionSqlType(expression,
        Collections.emptyMap());

    // Get the result
    final ColumnName fieldName = ColumnName.of("Result");
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of());
    final GenericExpressionResolver resolver = new GenericExpressionResolver(returnType,
        fieldName, executionContext.getMetaStore(), config, "insert value", false);
    final Object result =
        resolver.resolve(statement.getStatement().getExpression());
    
    return StatementExecutorResponse.handled(
        Optional.of(new ExpressionEvaluationEntity("result " + result.toString())));
  }
}
