package io.confluent.ksql.execution.evaluator;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public class Interpreter {


  public static ExpressionInterpreter create(final FunctionRegistry functionRegistry,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final Expression expression) {
    ExpressionTypeManager expressionTypeManager
        = new ExpressionTypeManager(schema, functionRegistry);
    final SqlType returnType = expressionTypeManager.getExpressionSqlType(expression);
    if (returnType == null) {
      throw new KsqlException("NULL expression not supported");
    }
    return new ExpressionInterpreter(functionRegistry, schema, ksqlConfig, expression, returnType);
  }
}
