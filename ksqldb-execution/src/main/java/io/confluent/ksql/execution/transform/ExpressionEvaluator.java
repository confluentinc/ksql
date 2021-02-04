package io.confluent.ksql.execution.transform;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.function.Supplier;

public interface ExpressionEvaluator {
  Object evaluate(
      final GenericRow row,
      final Object defaultValue,
      final ProcessingLogger logger,
      final Supplier<String> errorMsg
  );

  Expression getExpression();

  SqlType getExpressionType();
}
